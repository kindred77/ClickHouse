#include <Interpreters/orcaopt/pgopt/RelationParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

RelationParser::RelationParser()
{
	relation_provider = std::make_shared<RelationParser>();
}

int RelationParser::RTERangeTablePosn(PGParseState *pstate, PGRangeTblEntry *rte, int *sublevels_up)
{
	int			index;
	PGListCell   *l;

	if (sublevels_up)
		*sublevels_up = 0;

	while (pstate != NULL)
	{
		index = 1;
		foreach(l, pstate->p_rtable)
		{
			if (rte == (PGRangeTblEntry *) lfirst(l))
				return index;
			index++;
		}
		pstate = pstate->parentParseState;
		if (sublevels_up)
			(*sublevels_up)++;
		else
			break;
	}

	elog(ERROR, "RTE not found (internal error)");
	return 0;					/* keep compiler quiet */
};

void RelationParser::expandTupleDesc(TupleDesc tupdesc, PGAlias *eref, int count, int offset,
				int rtindex, int sublevels_up,
				int location, bool include_dropped,
				PGList **colnames, PGList **colvars)
{
	PGListCell   *aliascell = list_head(eref->colnames);
	int			varattno;

	if (colnames)
	{
		int			i;

		for (i = 0; i < offset; i++)
		{
			if (aliascell)
				aliascell = lnext(aliascell);
		}
	}

	Assert(count <= tupdesc->natts);
	for (varattno = 0; varattno < count; varattno++)
	{
		Form_pg_attribute attr = tupdesc->attrs[varattno];

		if (attr->attisdropped)
		{
			if (include_dropped)
			{
				if (colnames)
					*colnames = lappend(*colnames, makeString(pstrdup("")));
				if (colvars)
				{
					/*
					 * can't use atttypid here, but it doesn't really matter
					 * what type the Const claims to be.
					 */
					*colvars = lappend(*colvars,
									 makeNullConst(INT4OID, -1, InvalidOid));
				}
			}
			if (aliascell)
				aliascell = lnext(aliascell);
			continue;
		}

		if (colnames)
		{
			char	   *label;

			if (aliascell)
			{
				label = strVal(lfirst(aliascell));
				aliascell = lnext(aliascell);
			}
			else
			{
				/* If we run out of aliases, use the underlying name */
				label = attr->attname.data;
			}
			*colnames = lappend(*colnames, makeString(pstrdup(label)));
		}

		if (colvars)
		{
			PGVar		   *varnode;

			varnode = makeVar(rtindex, varattno + offset + 1,
							  attr->atttypid, attr->atttypmod,
							  attr->attcollation,
							  sublevels_up);
			varnode->location = location;

			*colvars = lappend(*colvars, varnode);
		}
	}
};

void RelationParser::expandRelation(Oid relid, PGAlias *eref, int rtindex, int sublevels_up,
			   int location, bool include_dropped,
			   PGList **colnames, PGList **colvars)
{
	Relation	rel;

	/* Get the tupledesc and turn it over to expandTupleDesc */
	rel = relation_open(relid, AccessShareLock);
	expandTupleDesc(rel->rd_att, eref, rel->rd_att->natts, 0,
					rtindex, sublevels_up,
					location, include_dropped,
					colnames, colvars);
	relation_close(rel, AccessShareLock);
};

void
RelationParser::expandRTE(PGRangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  PGList **colnames, PGList **colvars)
{
	int			varattno;

	if (colnames)
		*colnames = NIL;
	if (colvars)
		*colvars = NIL;

	switch (rte->rtekind)
	{
		case PG_RTE_RELATION:
			/* Ordinary relation RTE */
			expandRelation(rte->relid, rte->eref,
						   rtindex, sublevels_up, location,
						   include_dropped, colnames, colvars);
			break;
		case PG_RTE_SUBQUERY:
			{
				/* Subquery RTE */
				PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				PGListCell   *tlistitem;

				varattno = 0;
				foreach(tlistitem, rte->subquery->targetList)
				{
					PGTargetEntry *te = (PGTargetEntry *) lfirst(tlistitem);

					if (te->resjunk)
						continue;
					varattno++;
					Assert(varattno == te->resno);

					/*
					 * In scenarios where columns have been added to a view
					 * since the outer query was originally parsed, there can
					 * be more items in the subquery tlist than the outer
					 * query expects.  We should ignore such extra column(s)
					 * --- compare the behavior for composite-returning
					 * functions, in the RTE_FUNCTION case below.
					 */
					if (!aliasp_item)
						break;

					if (colnames)
					{
						char	   *label = strVal(lfirst(aliasp_item));

						*colnames = lappend(*colnames, makeString(pstrdup(label)));
					}

					if (colvars)
					{
						PGVar		   *varnode;

						varnode = makeVar(rtindex, varattno,
										  exprType((PGNode *) te->expr),
										  exprTypmod((PGNode *) te->expr),
										  exprCollation((PGNode *) te->expr),
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}

					aliasp_item = lnext(aliasp_item);
				}
			}
			break;
		case PG_RTE_TABLEFUNC:
		case PG_RTE_FUNCTION:
			{
				/* Function RTE */
				int			atts_done = 0;
				PGListCell   *lc;

				foreach(lc, rte->functions)
				{
					PGRangeTblFunction *rtfunc = (PGRangeTblFunction *) lfirst(lc);
					TypeFuncClass functypclass;
					Oid			funcrettype;
					TupleDesc	tupdesc;

					functypclass = get_expr_result_type(rtfunc->funcexpr,
														&funcrettype,
														&tupdesc);
					if (functypclass == TYPEFUNC_COMPOSITE)
					{
						/* Composite data type, e.g. a table's row type */
						Assert(tupdesc);
						expandTupleDesc(tupdesc, rte->eref,
										rtfunc->funccolcount, atts_done,
										rtindex, sublevels_up, location,
										include_dropped, colnames, colvars);
					}
					else if (functypclass == TYPEFUNC_SCALAR)
					{
						/* Base data type, i.e. scalar */
						if (colnames)
							*colnames = lappend(*colnames,
												list_nth(rte->eref->colnames,
														 atts_done));

						if (colvars)
						{
							PGVar		   *varnode;

							varnode = makeVar(rtindex, atts_done + 1,
											  funcrettype, -1,
											  exprCollation(rtfunc->funcexpr),
											  sublevels_up);
							varnode->location = location;

							*colvars = lappend(*colvars, varnode);
						}
					}
					else if (functypclass == TYPEFUNC_RECORD)
					{
						if (colnames)
						{
							PGList	   *namelist;

							/* extract appropriate subset of column list */
							namelist = list_copy_tail(rte->eref->colnames,
													  atts_done);
							namelist = list_truncate(namelist,
													 rtfunc->funccolcount);
							*colnames = list_concat(*colnames, namelist);
						}

						if (colvars)
						{
							PGListCell   *l1;
							PGListCell   *l2;
							PGListCell   *l3;
							int			attnum = atts_done;

							forthree(l1, rtfunc->funccoltypes,
									 l2, rtfunc->funccoltypmods,
									 l3, rtfunc->funccolcollations)
							{
								Oid			attrtype = lfirst_oid(l1);
								int32		attrtypmod = lfirst_int(l2);
								Oid			attrcollation = lfirst_oid(l3);
								PGVar		   *varnode;

								attnum++;
								varnode = makeVar(rtindex,
												  attnum,
												  attrtype,
												  attrtypmod,
												  attrcollation,
												  sublevels_up);
								varnode->location = location;
								*colvars = lappend(*colvars, varnode);
							}
						}
					}
					else
					{
						/* addRangeTableEntryForFunction should've caught this */
						elog(ERROR, "function in FROM has unsupported return type");
					}
					atts_done += rtfunc->funccolcount;
				}

				/* Append the ordinality column if any */
				if (rte->funcordinality)
				{
					if (colnames)
						*colnames = lappend(*colnames,
											llast(rte->eref->colnames));

					if (colvars)
					{
						PGVar      *varnode = makeVar(rtindex,
													  atts_done + 1,
													  INT8OID,
													  -1,
													  InvalidOid,
													  sublevels_up);

						*colvars = lappend(*colvars, varnode);
					}
				}
			}
			break;
		// case PG_RTE_VALUES:
		// 	{
		// 		/* Values RTE */
		// 		PGListCell   *aliasp_item = list_head(rte->eref->colnames);
		// 		int32	   *coltypmods;
		// 		PGListCell   *lcv;
		// 		PGListCell   *lcc;

		// 		/*
		// 		 * It's okay to extract column types from the expressions in
		// 		 * the first row, since all rows will have been coerced to the
		// 		 * same types.  Their typmods might not be the same though, so
		// 		 * we potentially need to examine all rows to compute those.
		// 		 * Column collations are pre-computed in values_collations.
		// 		 */
		// 		if (colvars)
		// 			coltypmods = getValuesTypmods(rte);
		// 		else
		// 			coltypmods = NULL;

		// 		varattno = 0;
		// 		forboth(lcv, (PGList *) linitial(rte->values_lists),
		// 				lcc, rte->values_collations)
		// 		{
		// 			PGNode	   *col = (PGNode *) lfirst(lcv);
		// 			Oid			colcollation = lfirst_oid(lcc);

		// 			varattno++;
		// 			if (colnames)
		// 			{
		// 				/* Assume there is one alias per column */
		// 				char	   *label = strVal(lfirst(aliasp_item));

		// 				*colnames = lappend(*colnames,
		// 									makeString(pstrdup(label)));
		// 				aliasp_item = lnext(aliasp_item);
		// 			}

		// 			if (colvars)
		// 			{
		// 				PGVar		   *varnode;

		// 				varnode = makeVar(rtindex, varattno,
		// 								  exprType(col),
		// 								  coltypmods[varattno - 1],
		// 								  colcollation,
		// 								  sublevels_up);
		// 				varnode->location = location;
		// 				*colvars = lappend(*colvars, varnode);
		// 			}
		// 		}
		// 		if (coltypmods)
		// 			pfree(coltypmods);
		// 	}
		// 	break;
		case PG_RTE_JOIN:
			{
				/* Join RTE */
				PGListCell   *colname;
				PGListCell   *aliasvar;

				Assert(list_length(rte->eref->colnames) == list_length(rte->joinaliasvars));

				varattno = 0;
				forboth(colname, rte->eref->colnames, aliasvar, rte->joinaliasvars)
				{
					PGNode	   *avar = (PGNode *) lfirst(aliasvar);

					varattno++;

					/*
					 * During ordinary parsing, there will never be any
					 * deleted columns in the join; but we have to check since
					 * this routine is also used by the rewriter, and joins
					 * found in stored rules might have join columns for
					 * since-deleted columns.  This will be signaled by a null
					 * pointer in the alias-vars list.
					 */
					if (avar == NULL)
					{
						if (include_dropped)
						{
							if (colnames)
								*colnames = lappend(*colnames,
													makeString(pstrdup("")));
							if (colvars)
							{
								/*
								 * Can't use join's column type here (it might
								 * be dropped!); but it doesn't really matter
								 * what type the Const claims to be.
								 */
								*colvars = lappend(*colvars,
												   makeNullConst(INT4OID, -1,
																 InvalidOid));
							}
						}
						continue;
					}

					if (colnames)
					{
						char	   *label = strVal(lfirst(colname));

						*colnames = lappend(*colnames,
											makeString(pstrdup(label)));
					}

					if (colvars)
					{
						PGVar		   *varnode;

						varnode = makeVar(rtindex, varattno,
										  exprType(avar),
										  exprTypmod(avar),
										  exprCollation(avar),
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}
				}
			}
			break;
		case PG_RTE_CTE:
			{
				PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				PGListCell   *lct;
				PGListCell   *lcm;
				PGListCell   *lcc;

				varattno = 0;
				forthree(lct, rte->ctecoltypes,
						 lcm, rte->ctecoltypmods,
						 lcc, rte->ctecolcollations)
				{
					Oid			coltype = lfirst_oid(lct);
					int32		coltypmod = lfirst_int(lcm);
					Oid			colcoll = lfirst_oid(lcc);

					varattno++;

					if (colnames)
					{
						/* Assume there is one alias per output column */
						char	   *label = strVal(lfirst(aliasp_item));

						*colnames = lappend(*colnames, makeString(pstrdup(label)));
						aliasp_item = lnext(aliasp_item);
					}

					if (colvars)
					{
						PGVar		   *varnode;

						varnode = makeVar(rtindex, varattno,
										  coltype, coltypmod, colcoll,
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}
				}
			}
			break;
		default:
			//elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
			throw Exception(ERROR, "unrecognized RTE kind: {}", rte->rtekind);
	}
};

void
RelationParser::checkNameSpaceConflicts(PGParseState *pstate, PGList *namespace1,
						PGList *namespace2)
{
	PGListCell   *l1;

	foreach(l1, namespace1)
	{
		PGParseNamespaceItem *nsitem1 = (PGParseNamespaceItem *) lfirst(l1);
		PGRangeTblEntry *rte1 = nsitem1->p_rte;
		const char *aliasname1 = rte1->eref->aliasname;
		PGListCell   *l2;

		if (!nsitem1->p_rel_visible)
			continue;

		foreach(l2, namespace2)
		{
			PGParseNamespaceItem *nsitem2 = (PGParseNamespaceItem *) lfirst(l2);
			PGRangeTblEntry *rte2 = nsitem2->p_rte;

			if (!nsitem2->p_rel_visible)
				continue;
			if (strcmp(rte2->eref->aliasname, aliasname1) != 0)
				continue;		/* definitely no conflict */
			if (rte1->rtekind == PGRTEKind::PG_RTE_RELATION && rte1->alias == NULL &&
				rte2->rtekind == PGRTEKind::PG_RTE_RELATION && rte2->alias == NULL &&
				rte1->relid != rte2->relid)
				continue;		/* no conflict per SQL rule */
			// ereport(ERROR,
			// 		(errcode(ERRCODE_DUPLICATE_ALIAS),
			// 		 errmsg("table name \"%s\" specified more than once",
			// 				aliasname1)));
			throw Exception(ERROR, "table name {} specified more than once", aliasname1);
		}
	}
};

PGRangeTblEntry *
RelationParser::addRangeTableEntryForCTE(PGParseState *pstate,
						 PGCommonTableExpr *cte,
						 Index levelsup,
						 PGRangeVar *rv,
						 bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	PGAlias	   *alias = rv->alias;
	char	   *refname = alias ? alias->aliasname : cte->ctename;
	PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	PGListCell   *lc;

	rte->rtekind = PG_RTE_CTE;
	rte->ctename = cte->ctename;
	rte->ctelevelsup = levelsup;

	/* Self-reference if and only if CTE's parse analysis isn't completed */
	rte->self_reference = !IsA(cte->ctequery, PGQuery);
	Assert(cte->cterecursive || !rte->self_reference);
	/* Bump the CTE's refcount if this isn't a self-reference */
	if (!rte->self_reference)
		cte->cterefcount++;

	/*
	 * We throw error if the CTE is INSERT/UPDATE/DELETE without RETURNING.
	 * This won't get checked in case of a self-reference, but that's OK
	 * because data-modifying CTEs aren't allowed to be recursive anyhow.
	 */
	if (IsA(cte->ctequery, PGQuery))
	{
		PGQuery *ctequery = (PGQuery *) cte->ctequery;

		if (ctequery->commandType != PGCmdType::PG_CMD_SELECT &&
			ctequery->returningList == NIL)
			// ereport(ERROR,
			// 		(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			// 	 errmsg("WITH query \"%s\" does not have a RETURNING clause",
			// 			cte->ctename),
			// 		 parser_errposition(pstate, rv->location)));
			throw Exception(ERROR, "WITH query {} does not have a RETURNING clause", cte->ctename);
	}

	rte->ctecoltypes = cte->ctecoltypes;
	rte->ctecoltypmods = cte->ctecoltypmods;
	rte->ctecolcollations = cte->ctecolcollations;

	rte->alias = alias;
	if (alias)
		eref = reinterpret_cast<PGAlias *>(copyObject(alias));
	else
		eref = makeAlias(refname, NIL);
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	varattno = 0;
	foreach(lc, cte->ctecolnames)
	{
		varattno++;
		if (varattno > numaliases)
			eref->colnames = lappend(eref->colnames, lfirst(lc));
	}
	if (varattno < numaliases)
		// ereport(ERROR,
		// 		(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		// 		 errmsg("table \"%s\" has %d columns available but %d columns specified",
		// 				refname, varattno, numaliases)));
		throw Exception(ERROR, "table {} has {} columns available but {} columns specified", refname, varattno, numaliases);

	rte->eref = eref;

	/*
	 * Set flags and access permissions.
	 *
	 * Subqueries are never checked for access rights.
	 */
	rte->lateral = false;
	rte->inh = false;			/* never true for subqueries */
	rte->inFromCl = inFromCl;

	rte->requiredPerms = 0;
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->modifiedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

PGCommonTableExpr *
RelationParser::scanNameSpaceForCTE(PGParseState *pstate, const char *refname,
					Index *ctelevelsup)
{
	Index		levelsup;

	for (levelsup = 0;
		 pstate != NULL;
		 pstate = pstate->parentParseState, levelsup++)
	{
		PGListCell   *lc;

		foreach(lc, pstate->p_ctenamespace)
		{
			PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);

			if (strcmp(cte->ctename, refname) == 0)
			{
				*ctelevelsup = levelsup;
				return cte;
			}
		}
	}
	return NULL;
};

void
RelationParser::get_rte_attribute_type(PGRangeTblEntry *rte, PGAttrNumber attnum,
					   Oid *vartype, int32 *vartypmod, Oid *varcollid)
{
	switch (rte->rtekind)
	{
		case PG_RTE_RELATION:
			{
				/* Plain relation RTE --- get the attribute's type info */
				HeapTuple	tp;
				Form_pg_attribute att_tup;

				tp = SearchSysCache2(ATTNUM,
									 ObjectIdGetDatum(rte->relid),
									 Int16GetDatum(attnum));
				if (!HeapTupleIsValid(tp))		/* shouldn't happen */
					elog(ERROR, "cache lookup failed for attribute %d of relation %u",
						 attnum, rte->relid);
				att_tup = (Form_pg_attribute) GETSTRUCT(tp);

				/*
				 * If dropped column, pretend it ain't there.  See notes in
				 * scanRTEForColumn.
				 */
				if (att_tup->attisdropped)
					ereport(ERROR,
							(errcode(PG_ERRCODE_SYNTAX_ERROR),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   NameStr(att_tup->attname),
						   get_rel_name(rte->relid))));
				*vartype = att_tup->atttypid;
				*vartypmod = att_tup->atttypmod;
				*varcollid = att_tup->attcollation;
				ReleaseSysCache(tp);
			}
			break;
		case PG_RTE_SUBQUERY:
			{
				/* Subselect RTE --- get type info from subselect's tlist */
				PGTargetEntry *te = get_tle_by_resno(rte->subquery->targetList,
												   attnum);

				if (te == NULL || te->resjunk)
					elog(ERROR, "subquery %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				*vartype = exprType((PGNode *) te->expr);
				*vartypmod = exprTypmod((PGNode *) te->expr);
				*varcollid = exprCollation((PGNode *) te->expr);
			}
			break;
		//case PG_RTE_TABLEFUNCTION:
		case PG_RTE_FUNCTION:
			{
				/* Function RTE */
				PGListCell   *lc;
				int			atts_done = 0;

				/* Identify which function covers the requested column */
				foreach(lc, rte->functions)
				{
					PGRangeTblFunction *rtfunc = (PGRangeTblFunction *) lfirst(lc);

					if (attnum > atts_done &&
						attnum <= atts_done + rtfunc->funccolcount)
					{
						TypeFuncClass functypclass;
						Oid			funcrettype;
						TupleDesc	tupdesc;

						attnum -= atts_done;	/* now relative to this func */
						functypclass = get_expr_result_type(rtfunc->funcexpr,
															&funcrettype,
															&tupdesc);

						if (functypclass == TYPEFUNC_COMPOSITE)
						{
							/* Composite data type, e.g. a table's row type */
							Form_pg_attribute att_tup;

							Assert(tupdesc);
							Assert(attnum <= tupdesc->natts);
							att_tup = tupdesc->attrs[attnum - 1];

							/*
							 * If dropped column, pretend it ain't there.  See
							 * notes in scanRTEForColumn.
							 */
							if (att_tup->attisdropped)
								// ereport(ERROR,
								// 		(errcode(ERRCODE_UNDEFINED_COLUMN),
								// 		 errmsg("column \"%s\" of relation \"%s\" does not exist",
								// 				NameStr(att_tup->attname),
								// 				rte->eref->aliasname)));
								throw Exception(ERROR, "column {} of relation {} does not exist", att_tup->attname.data, rte->eref->aliasname);
							*vartype = att_tup->atttypid;
							*vartypmod = att_tup->atttypmod;
							*varcollid = att_tup->attcollation;
						}
						else if (functypclass == TYPEFUNC_SCALAR)
						{
							/* Base data type, i.e. scalar */
							*vartype = funcrettype;
							*vartypmod = -1;
							*varcollid = exprCollation(rtfunc->funcexpr);
						}
						else if (functypclass == TYPEFUNC_RECORD)
						{
							*vartype = list_nth_oid(rtfunc->funccoltypes,
													attnum - 1);
							*vartypmod = list_nth_int(rtfunc->funccoltypmods,
													  attnum - 1);
							*varcollid = list_nth_oid(rtfunc->funccolcollations,
													  attnum - 1);
						}
						else
						{
							/*
							 * addRangeTableEntryForFunction should've caught
							 * this
							 */
							//elog(ERROR, "function in FROM has unsupported return type");
							throw Exception(ERROR, "function in FROM has unsupported return type");
						}
						return;
					}
					atts_done += rtfunc->funccolcount;
				}

				/* If we get here, must be looking for the ordinality column */
				if (rte->funcordinality && attnum == atts_done + 1)
				{
					*vartype = INT8OID;
					*vartypmod = -1;
					*varcollid = InvalidOid;
					return;
				}

				/* this probably can't happen ... */
				// ereport(ERROR,
				// 		(errcode(ERRCODE_UNDEFINED_COLUMN),
				// 		 errmsg("column %d of relation \"%s\" does not exist",
				// 				attnum,
				// 				rte->eref->aliasname)));
				throw Exception(ERROR, "column %d of relation \"%s\" does not exist",
					attnum, rte->eref->aliasname);
			}
			break;
		case PG_RTE_VALUES:
			{
				/*
				 * Values RTE --- we can get type info from first sublist, but
				 * typmod may require scanning all sublists, and collation is
				 * stored separately.  Using getValuesTypmods() is overkill,
				 * but this path is taken so seldom for VALUES that it's not
				 * worth writing extra code.
				 */
				PGList	   *collist = (PGList *) linitial(rte->values_lists);
				PGNode	   *col;
				int32	   *coltypmods = getValuesTypmods(rte);

				if (attnum < 1 || attnum > list_length(collist))
					elog(ERROR, "values list %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				col = (PGNode *) list_nth(collist, attnum - 1);
				*vartype = exprType(col);
				*vartypmod = coltypmods[attnum - 1];
				*varcollid = list_nth_oid(rte->values_collations, attnum - 1);
				pfree(coltypmods);
			}
			break;
		case PG_RTE_JOIN:
			{
				/*
				 * Join RTE --- get type info from join RTE's alias variable
				 */
				PGNode	   *aliasvar;

				Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
				aliasvar = (PGNode *) list_nth(rte->joinaliasvars, attnum - 1);
				Assert(aliasvar != NULL);
				*vartype = exprType(aliasvar);
				*vartypmod = exprTypmod(aliasvar);
				*varcollid = exprCollation(aliasvar);
			}
			break;
		// case PG_RTE_CTE:
		// 	{
		// 		/* CTE RTE --- get type info from lists in the RTE */
		// 		Assert(attnum > 0 && attnum <= list_length(rte->ctecoltypes));
		// 		*vartype = list_nth_oid(rte->ctecoltypes, attnum - 1);
		// 		*vartypmod = list_nth_int(rte->ctecoltypmods, attnum - 1);
		// 		*varcollid = list_nth_oid(rte->ctecolcollations, attnum - 1);
		// 	}
		// 	break;
		default:
			//elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
			throw Exception(ERROR, "unrecognized RTE kind: {}", (int) rte->rtekind);
	}
}

PGVar *
RelationParser::make_var(PGParseState *pstate, PGRangeTblEntry *rte, int attrno, int location)
{
	PGVar		   *result;
	int			vnum,
				sublevels_up;
	Oid			vartypeid;
	int32		type_mod;
	Oid			varcollid;

	vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);
	get_rte_attribute_type(rte, attrno, &vartypeid, &type_mod, &varcollid);
	result = makeVar(vnum, attrno, vartypeid, type_mod, varcollid, sublevels_up);
	result->location = location;
	return result;
}

PGNode *
RelationParser::scanRTEForColumn(PGParseState *pstate, PGRangeTblEntry *rte, char *colname,
				 int location)
{
	PGNode	   *result = NULL;
	int			attnum = 0;
	PGVar		   *var;
	PGListCell   *c;

	/*
	 * Scan the user column names (or aliases) for a match. Complain if
	 * multiple matches.
	 *
	 * Note: eref->colnames may include entries for dropped columns, but those
	 * will be empty strings that cannot match any legal SQL identifier, so we
	 * don't bother to test for that case here.
	 *
	 * Should this somehow go wrong and we try to access a dropped column,
	 * we'll still catch it by virtue of the checks in
	 * get_rte_attribute_type(), which is called by make_var().  That routine
	 * has to do a cache lookup anyway, so the check there is cheap.
	 */
	foreach(c, rte->eref->colnames)
	{
		attnum++;
		if (strcmp(strVal(lfirst(c)), colname) == 0)
		{
			if (result)
				// ereport(ERROR,
				// 		(errcode(ERRCODE_AMBIGUOUS_COLUMN),
				// 		 errmsg("column reference \"%s\" is ambiguous",
				// 				colname),
				// 		 parser_errposition(pstate, location)));
				throw Exception(ERROR, "column reference {} is ambiguous", colname);
			var = make_var(pstate, rte, attnum, location);
			/* Require read access to the column */
			markVarForSelectPriv(pstate, var, rte);
			result = (PGNode *) var;
		}
	}

	/*
	 * If we have a unique match, return it.  Note that this allows a user
	 * alias to override a system column name (such as OID) without error.
	 */
	if (result)
		return result;

	/*
	 * If the RTE represents a real table, consider system column names.
	 */
	if (rte->rtekind == PG_RTE_RELATION)
	{
		/* In GPDB, system columns like gp_segment_id, ctid, xmin/xmax seem to be
		 * ambiguous for replicated table, replica in each segment has different
		 * value of those columns, between sessions, different replicas are choosen
		 * to provide data, so it's weird for users to see different system columns
		 * between sessions. So for replicated table, we don't expose system columns
		 * unless it's GP_ROLE_UTILITY for debug purpose.
		 */
		if (GpPolicyIsReplicated(GpPolicyFetch(rte->relid)) &&
			Gp_role != GP_ROLE_UTILITY)
			return result;

		/* quick check to see if name could be a system column */
		attnum = specialAttNum(colname);

		/* In constraint check, no system column is allowed except tableOid */
		/*
		 * In GPDB, tableoid is not allowed either, because we've removed
		 * HeapTupleData.t_tableOid field.
		 * GPDB_94_MERGE_FIXME: Could we make that work somehow? Resurrect
		 * t_tableOid, maybe? I think we'd need it for logical decoding as well.
		 */
		if (pstate->p_expr_kind == PGParseExprKind::EXPR_KIND_CHECK_CONSTRAINT &&
			attnum < InvalidAttrNumber /* && attnum != TableOidAttributeNumber */)
			// ereport(ERROR,
			// 		(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
			// 		 errmsg("system column \"%s\" reference in check constraint is invalid",
			// 				colname),
			// 		 parser_errposition(pstate, location)));
			throw Exception(ERROR, "system column {} reference in check constraint is invalid", colname);

		if (attnum != InvalidAttrNumber)
		{
			/*
			 * Now check to see if column actually is defined.  Because of
			 * an ancient oversight in DefineQueryRewrite, it's possible that
			 * pg_attribute contains entries for system columns for a view,
			 * even though views should not have such --- so we also check
			 * the relkind.  This kluge will not be needed in 9.3 and later.
			 */
			if (SearchSysCacheExists2(ATTNUM,
									  ObjectIdGetDatum(rte->relid),
									  Int16GetDatum(attnum)) &&
				get_rel_relkind(rte->relid) != RELKIND_VIEW)
			{
				var = make_var(pstate, rte, attnum, location);
				/* Require read access to the column */
				markVarForSelectPriv(pstate, var, rte);
				result = (PGNode *) var;
			}
		}
	}

	return result;
};

void
RelationParser::buildRelationAliases(StoragePtr storage_ptr,
		PGAlias *alias, PGAlias *eref)
{
	int			maxattrs = storage_ptr->getColumns().size();
	PGListCell   *aliaslc;
	int			numaliases;
	int			varattno;
	int			numdropped = 0;

	Assert(eref->colnames == NIL);

	if (alias)
	{
		aliaslc = list_head(alias->colnames);
		numaliases = list_length(alias->colnames);
		/* We'll rebuild the alias colname list */
		alias->colnames = NIL;
	}
	else
	{
		aliaslc = NULL;
		numaliases = 0;
	}

	// for (varattno = 0; varattno < maxattrs; varattno++)
	// {
	// 	Form_pg_attribute attr = tupdesc->attrs[varattno];
	// 	PGValue	   *attrname;

	// 	if (attr->attisdropped)
	// 	{
	// 		/* Always insert an empty string for a dropped column */
	// 		attrname = makeString(pstrdup(""));
	// 		if (aliaslc)
	// 			alias->colnames = lappend(alias->colnames, attrname);
	// 		numdropped++;
	// 	}
	// 	else if (aliaslc)
	// 	{
	// 		/* Use the next user-supplied alias */
	// 		attrname = (PGValue *) lfirst(aliaslc);
	// 		aliaslc = lnext(aliaslc);
	// 		alias->colnames = lappend(alias->colnames, attrname);
	// 	}
	// 	else
	// 	{
	// 		attrname = makeString(pstrdup(attr->attname.data));
	// 		/* we're done with the alias if any */
	// 	}

	// 	eref->colnames = lappend(eref->colnames, attrname);
	// }

	foreach (auto name_and_type : storage_ptr->getColumns().getAll())
	{
		PGValue	   *attrname;

		if (attr->attisdropped)
		{
			/* Always insert an empty string for a dropped column */
			attrname = makeString(pstrdup(""));
			if (aliaslc)
				alias->colnames = lappend(alias->colnames, attrname);
			numdropped++;
		}
		else if (aliaslc)
		{
			/* Use the next user-supplied alias */
			attrname = (PGValue *) lfirst(aliaslc);
			aliaslc = lnext(aliaslc);
			alias->colnames = lappend(alias->colnames, attrname);
		}
		else
		{
			attrname = makeString(pstrdup(name_and_type.name.c_str()));
			/* we're done with the alias if any */
		}

		eref->colnames = lappend(eref->colnames, attrname);
	}

	/* Too many user-supplied aliases? */
	if (aliaslc)
		// ereport(ERROR,
		// 		(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		// 		 errmsg("table \"%s\" has %d columns available but %d columns specified",
		// 				eref->aliasname, maxattrs - numdropped, numaliases)));
		throw Exception(ERROR, "table {} has {} columns available but {} columns specified",
			eref->aliasname, maxattrs - numdropped, numaliases);
};

PGRangeTblEntry *
RelationParser::addRangeTableEntry(PGParseState *pstate,
				   PGRangeVar *relation,
				   PGAlias *alias,
				   bool inh,
				   bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	char	   *refname = alias ? alias->aliasname : relation->relname;
	//LOCKMODE	lockmode = AccessShareLock;
	PGLockingClause *locking;
	//Relation	rel;
	//ParseCallbackState pcbstate;

	rte->alias = alias;
	rte->rtekind = PG_RTE_RELATION;

	/*
	 * CDB: lock promotion around the locking clause is a little different
	 * from postgres to allow for required lock promotion for distributed
	 * AO tables.
	 * select for update should lock the whole table, we do it here.
	 */
	// locking = getLockedRefname(pstate, refname);
	// if (locking)
	// {
	// 	if (locking->strength >= LCS_FORNOKEYUPDATE)
	// 	{
	// 		Oid relid;

	// 		relid = RangeVarGetRelid(relation, lockmode, false);

	// 		rel = try_heap_open(relid, NoLock, true);
	// 		if (!rel)
	// 			//elog(ERROR, "open relation(%u) fail", relid);
	// 			throw Exception(ERROR, "open relation({}) fail", relid);

	// 		if (rel->rd_rel->relkind == RELKIND_MATVIEW)
	// 			// ereport(ERROR,
	// 			// 		(errcode(ERRCODE_WRONG_OBJECT_TYPE),
	// 			// 				errmsg("cannot lock rows in materialized view \"%s\"",
	// 			// 					   RelationGetRelationName(rel))));
	// 			throw Exception(ERROR, "cannot lock rows in materialized view {}", RelationGetRelationName(rel));

	// 		lockmode = IsSystemRelation(rel) ? RowExclusiveLock : ExclusiveLock;
	// 		heap_close(rel, NoLock);
	// 	}
	// 	else
	// 	{
	// 		lockmode = RowShareLock;
	// 	}
	// }

	/*
	 * Get the rel's OID.  This access also ensures that we have an up-to-date
	 * relcache entry for the rel.  Since this is typically the first access
	 * to a rel in a statement, be careful to get the right access level
	 * depending on whether we're doing SELECT FOR UPDATE/SHARE.
	 */
	//setup_parser_errposition_callback(&pcbstate, pstate, relation->location);
	//rel = parserOpenTable(pstate, relation, lockmode, NULL);
	//cancel_parser_errposition_callback(&pcbstate);

	auto item = relation_provider->getPairByDBAndTableName(relation->schemaname, relation->relname);
	if (!item)
	{
		throw Exception(ERROR, "can not find relation {}", relation->relname);
	}
	rte->relid = std::get<0>(item);
	rte->relkind = std::get<2>(item);
	/*
	 * Build the list of effective column names using user-supplied aliases
	 * and/or actual column names.
	 */
	rte->eref = makeAlias(refname, NIL);
	buildRelationAliases(std::get<1>(item), alias, rte->eref);

	/*
	 * Drop the rel refcount, but keep the access lock till end of transaction
	 * so that the table can't be deleted or have its schema modified
	 * underneath us.
	 */
	//heap_close(rel, NoLock);

	/*
	 * Set flags and access permissions.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 */
	rte->lateral = false;
	rte->inh = inh;
	rte->inFromCl = inFromCl;

	//rte->requiredPerms = ACL_SELECT;
	//rte->checkAsUser = InvalidOid;		/* not set-uid by default, either */
	//rte->selectedCols = NULL;
	//rte->modifiedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

PGRangeTblEntry *
RelationParser::addRangeTableEntryForSubquery(PGParseState *pstate,
							  PGQuery *subquery,
							  PGAlias *alias,
							  bool lateral,
							  bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	char	   *refname = alias->aliasname;
	PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	PGListCell   *tlistitem;

	rte->rtekind = PG_RTE_SUBQUERY;
	rte->relid = InvalidOid;
	rte->subquery = subquery;
	rte->alias = alias;

	eref = reinterpret_cast<PGAlias*>(copyObject(alias));
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	varattno = 0;
	foreach(tlistitem, subquery->targetList)
	{
		PGTargetEntry *te = (PGTargetEntry *) lfirst(tlistitem);

		if (te->resjunk)
			continue;
		varattno++;
		Assert(varattno == te->resno);
		if (varattno > numaliases)
		{
			char	   *attrname;

			attrname = pstrdup(te->resname);
			eref->colnames = lappend(eref->colnames, makeString(attrname));
		}
	}
	if (varattno < numaliases)
		//ereport(ERROR,
				//(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 //errmsg("table \"%s\" has %d columns available but %d columns specified",
						//refname, varattno, numaliases)));
		throw Exception(ERROR, "table {} has {} columns available but {} columns specified.",
			refname, varattno, numaliases);

	rte->eref = eref;

	/*
	 * Set flags and access permissions.
	 *
	 * Subqueries are never checked for access rights.
	 */
	rte->lateral = lateral;
	rte->inh = false;			/* never true for subqueries */
	rte->inFromCl = inFromCl;

	//rte->requiredPerms = 0;
	//rte->checkAsUser = InvalidOid;
	//rte->selectedCols = NULL;
	//rte->modifiedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
}

void RelationParser::markRTEForSelectPriv(PGParseState *pstate, PGRangeTblEntry *rte,
					 int rtindex, PGAttrNumber col)
{
	if (rte == NULL)
		rte = rt_fetch(rtindex, pstate->p_rtable);

	if (rte->rtekind == PG_RTE_RELATION)
	{
		/* Make sure the rel as a whole is marked for SELECT access */
		//rte->requiredPerms |= PG_ACL_SELECT;
		/* Must offset the attnum to fit in a bitmapset */
		//rte->selectedCols = bms_add_member(rte->selectedCols,
								   //col - FirstLowInvalidHeapAttributeNumber);
	}
	else if (rte->rtekind == PG_RTE_JOIN)
	{
		if (col == InvalidAttrNumber)
		{
			/*
			 * A whole-row reference to a join has to be treated as whole-row
			 * references to the two inputs.
			 */
			PGJoinExpr   *j;

			if (rtindex > 0 && rtindex <= list_length(pstate->p_joinexprs))
				j = (PGJoinExpr *) list_nth(pstate->p_joinexprs, rtindex - 1);
			else
				j = NULL;
			if (j == NULL)
				//elog(ERROR, "could not find JoinExpr for whole-row reference");
				throw Exception(ERROR, "could not find JoinExpr for whole-row reference");
			Assert(IsA(j, PGJoinExpr));

			/* Note: we can't see FromExpr here */
			if (IsA(j->larg, PGRangeTblRef))
			{
				int			varno = ((PGRangeTblRef *) j->larg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else if (IsA(j->larg, PGJoinExpr))
			{
				int			varno = ((PGJoinExpr *) j->larg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else
				// elog(ERROR, "unrecognized node type: %d",
				// 	 (int) nodeTag(j->larg));
				throw Exception(ERROR, "unrecognized node type: {}", (int) nodeTag(j->larg));
			if (IsA(j->rarg, PGRangeTblRef))
			{
				int			varno = ((PGRangeTblRef *) j->rarg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else if (IsA(j->rarg, PGJoinExpr))
			{
				int			varno = ((PGJoinExpr *) j->rarg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else
				// elog(ERROR, "unrecognized node type: %d",
				// 	 (int) nodeTag(j->rarg));
				throw Exception(ERROR, "unrecognized node type: {}", (int) nodeTag(j->larg));
		}
		else
		{
			/*
			 * Regular join attribute, look at the alias-variable list.
			 *
			 * The aliasvar could be either a Var or a COALESCE expression,
			 * but in the latter case we should already have marked the two
			 * referent variables as being selected, due to their use in the
			 * JOIN clause.  So we need only be concerned with the Var case.
			 * But we do need to drill down through implicit coercions.
			 */
			PGVar		   *aliasvar;

			Assert(col > 0 && col <= list_length(rte->joinaliasvars));
			aliasvar = (PGVar *) list_nth(rte->joinaliasvars, col - 1);
			aliasvar = (PGVar *) strip_implicit_coercions((PGNode *) aliasvar);
			if (aliasvar && IsA(aliasvar, PGVar))
				markVarForSelectPriv(pstate, aliasvar, NULL);
		}
	}
};

void 
RelationParser::markVarForSelectPriv(PGParseState *pstate, PGVar *var, PGRangeTblEntry *rte)
{
	Index		lv;

	Assert(IsA(var, PGVar));
	/* Find the appropriate pstate if it's an uplevel Var */
	for (lv = 0; lv < var->varlevelsup; lv++)
		pstate = pstate->parentParseState;
	markRTEForSelectPriv(pstate, rte, var->varno, var->varattno);
}

PGNode *
RelationParser::colNameToVar(PGParseState *pstate, char *colname, bool localonly,
			 int location)
{
	PGNode	   *result = NULL;
	PGParseState *orig_pstate = pstate;

	while (pstate != NULL)
	{
		PGListCell   *l;

		foreach(l, pstate->p_namespace)
		{
			PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
			PGRangeTblEntry *rte = nsitem->p_rte;
			PGNode	   *newresult;

			/* Ignore table-only items */
			if (!nsitem->p_cols_visible)
				continue;
			/* If not inside LATERAL, ignore lateral-only items */
			if (nsitem->p_lateral_only && !pstate->p_lateral_active)
				continue;

			/* use orig_pstate here to get the right sublevels_up */
			newresult = scanRTEForColumn(orig_pstate, rte, colname, location);

			if (newresult)
			{
				if (result)
					//ereport(ERROR,
							//(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							// errmsg("column reference \"%s\" is ambiguous",
							//		colname),
							// parser_errposition(pstate, location)));
				throw Exception(ERROR, "column reference {} is ambiguous.",
					colname);
				check_lateral_ref_ok(pstate, nsitem, location);
				result = newresult;
			}
		}

		if (result != NULL || localonly)
			break;				/* found, or don't want to look at parent */

		pstate = pstate->parentParseState;
	}

	return result;
};

PGRangeTblEntry *
RelationParser::GetRTEByRangeTablePosn(PGParseState *pstate,
					   int varno,
					   int sublevels_up)
{
	while (sublevels_up-- > 0)
	{
		pstate = pstate->parentParseState;
		Assert(pstate != NULL);
	}
	Assert(varno > 0 && varno <= list_length(pstate->p_rtable));
	return rt_fetch(varno, pstate->p_rtable);
};

PGCommonTableExpr *
RelationParser::GetCTEForRTE(PGParseState *pstate, PGRangeTblEntry *rte, int rtelevelsup)
{
	Index		levelsup;
	PGListCell   *lc;

	/* Determine RTE's levelsup if caller didn't know it */
	if (rtelevelsup < 0)
		(void) RTERangeTablePosn(pstate, rte, &rtelevelsup);

	Assert(rte->rtekind == PG_RTE_CTE);
	levelsup = rte->ctelevelsup + rtelevelsup;
	while (levelsup-- > 0)
	{
		pstate = pstate->parentParseState;
		if (!pstate)			/* shouldn't happen */
			//elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
			throw Exception(ERROR, "bad levelsup for CTE {}", rte->ctename);
	}
	foreach(lc, pstate->p_ctenamespace)
	{
		PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);

		if (strcmp(cte->ctename, rte->ctename) == 0)
			return cte;
	}
	/* shouldn't happen */
	//elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
	throw Exception(ERROR, "could not find CTE {}", rte->ctename);
	//return NULL;				/* keep compiler quiet */
};

PGRangeTblEntry *
RelationParser::addRangeTableEntryForJoin(PGParseState *pstate,
						  PGList *colnames,
						  PGJoinType jointype,
						  PGList *aliasvars,
						  PGAlias *alias,
						  bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	PGAlias	   *eref;
	int			numaliases;

	/*
	 * Fail if join has too many columns --- we must be able to reference any
	 * of the columns with an AttrNumber.
	 */
	if (list_length(aliasvars) > MaxAttrNumber)
		// ereport(ERROR,
		// 		(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		// 		 errmsg("joins can have at most %d columns",
		// 				MaxAttrNumber)));
		throw Exception(ERROR, "joins can have at most {} columns", MaxAttrNumber);

	rte->rtekind = PG_RTE_JOIN;
	rte->relid = InvalidOid;
	rte->subquery = NULL;
	rte->jointype = jointype;
	rte->joinaliasvars = aliasvars;
	rte->alias = alias;

	/* transform any Vars of type UNKNOWNOID if we can */
	coerce_parser.fixup_unknown_vars_in_exprlist(pstate, rte->joinaliasvars);

	eref = alias ? (PGAlias *) copyObject(alias) : makeAlias("unnamed_join", NIL);
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	if (numaliases < list_length(colnames))
		eref->colnames = list_concat(eref->colnames,
									 list_copy_tail(colnames, numaliases));

	rte->eref = eref;

	/*
	 * Set flags and access permissions.
	 *
	 * Joins are never checked for access rights.
	 */
	rte->lateral = false;
	rte->inh = false;			/* never true for joins */
	rte->inFromCl = inFromCl;

	// rte->requiredPerms = 0;
	// rte->checkAsUser = InvalidOid;
	// rte->selectedCols = NULL;
	// rte->modifiedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

PGTargetEntry *
RelationParser::get_tle_by_resno(PGList *tlist, PGAttrNumber resno)
{
	PGListCell   *l;

	foreach(l, tlist)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(l);

		if (tle->resno == resno)
			return tle;
	}
	return NULL;
};

duckdb_libpgquery::PGRangeTblEntry *
RelationParser::refnameRangeTblEntry(PGParseState *pstate,
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up)
{
	Oid			relId = InvalidOid;

	if (sublevels_up)
		*sublevels_up = 0;

	if (schemaname != NULL)
	{
		Oid			namespaceId;

		/*
		 * We can use LookupNamespaceNoError() here because we are only
		 * interested in finding existing RTEs.  Checking USAGE permission on
		 * the schema is unnecessary since it would have already been checked
		 * when the RTE was made.  Furthermore, we want to report "RTE not
		 * found", not "no permissions for schema", if the name happens to
		 * match a schema name the user hasn't got access to.
		 */
		namespaceId = LookupNamespaceNoError(schemaname);
		if (namespaceId == InvalidOid)
			return NULL;
		relId = get_relname_relid(refname, namespaceId);
		if (relId == InvalidOid)
			return NULL;
	}

	while (pstate != NULL)
	{
		PGRangeTblEntry *result;

		if (relId != InvalidOid)
			result = scanNameSpaceForRelid(pstate, relId, location);
		else
			result = scanNameSpaceForRefname(pstate, refname, location);

		if (result)
			return result;

		if (sublevels_up)
			(*sublevels_up)++;
		else
			break;

		pstate = pstate->parentParseState;
	}
	return NULL;
};

}