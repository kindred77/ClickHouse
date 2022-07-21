#include <RelationParser.h>

namespace DB
{

int RelationParser::RTERangeTablePosn(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int *sublevels_up)
{
	int			index;
	duckdb_libpgquery::PGListCell   *l;

	if (sublevels_up)
		*sublevels_up = 0;

	while (pstate != NULL)
	{
		index = 1;
		foreach(l, pstate->p_rtable)
		{
			if (rte == (duckdb_libpgquery::PGRangeTblEntry *) lfirst(l))
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

void RelationParser::expandTupleDesc(TupleDesc tupdesc, duckdb_libpgquery::PGAlias *eref, int count, int offset,
				int rtindex, int sublevels_up,
				int location, bool include_dropped,
				duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars)
{
	duckdb_libpgquery::PGListCell   *aliascell = list_head(eref->colnames);
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
					*colnames = lappend(*colnames, duckdb_libpgquery::makeString(duckdb_libpgquery::pstrdup("")));
				if (colvars)
				{
					/*
					 * can't use atttypid here, but it doesn't really matter
					 * what type the Const claims to be.
					 */
					*colvars = lappend(*colvars,
									 duckdb_libpgquery::makeNullConst(INT4OID, -1, InvalidOid));
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
			*colnames = lappend(*colnames, duckdb_libpgquery::makeString(duckdb_libpgquery::pstrdup(label)));
		}

		if (colvars)
		{
			duckdb_libpgquery::PGVar		   *varnode;

			varnode = duckdb_libpgquery::makeVar(rtindex, varattno + offset + 1,
							  attr->atttypid, attr->atttypmod,
							  attr->attcollation,
							  sublevels_up);
			varnode->location = location;

			*colvars = lappend(*colvars, varnode);
		}
	}
};

void RelationParser::expandRelation(Oid relid, duckdb_libpgquery::PGAlias *eref, int rtindex, int sublevels_up,
			   int location, bool include_dropped,
			   duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars)
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
RelationParser::expandRTE(duckdb_libpgquery::PGRangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars)
{
	int			varattno;

	if (colnames)
		*colnames = NIL;
	if (colvars)
		*colvars = NIL;

	switch (rte->rtekind)
	{
		case duckdb_libpgquery::PG_RTE_RELATION:
			/* Ordinary relation RTE */
			expandRelation(rte->relid, rte->eref,
						   rtindex, sublevels_up, location,
						   include_dropped, colnames, colvars);
			break;
		case duckdb_libpgquery::PG_RTE_SUBQUERY:
			{
				/* Subquery RTE */
				duckdb_libpgquery::PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				duckdb_libpgquery::PGListCell   *tlistitem;

				varattno = 0;
				foreach(tlistitem, rte->subquery->targetList)
				{
					duckdb_libpgquery::PGTargetEntry *te = (duckdb_libpgquery::PGTargetEntry *) lfirst(tlistitem);

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

						*colnames = lappend(*colnames, duckdb_libpgquery::makeString(duckdb_libpgquery::pstrdup(label)));
					}

					if (colvars)
					{
						duckdb_libpgquery::PGVar		   *varnode;

						varnode = duckdb_libpgquery::makeVar(rtindex, varattno,
										  duckdb_libpgquery::exprType((duckdb_libpgquery::PGNode *) te->expr),
										  duckdb_libpgquery::exprTypmod((duckdb_libpgquery::PGNode *) te->expr),
										  duckdb_libpgquery::exprCollation((duckdb_libpgquery::PGNode *) te->expr),
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}

					aliasp_item = lnext(aliasp_item);
				}
			}
			break;
		case duckdb_libpgquery::PG_RTE_TABLEFUNC:
		case duckdb_libpgquery::PG_RTE_FUNCTION:
			{
				/* Function RTE */
				int			atts_done = 0;
				duckdb_libpgquery::PGListCell   *lc;

				foreach(lc, rte->functions)
				{
					duckdb_libpgquery::PGRangeTblFunction *rtfunc = (duckdb_libpgquery::PGRangeTblFunction *) lfirst(lc);
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
							duckdb_libpgquery::PGVar		   *varnode;

							varnode = duckdb_libpgquery::makeVar(rtindex, atts_done + 1,
											  funcrettype, -1,
											  duckdb_libpgquery::exprCollation(rtfunc->funcexpr),
											  sublevels_up);
							varnode->location = location;

							*colvars = lappend(*colvars, varnode);
						}
					}
					else if (functypclass == TYPEFUNC_RECORD)
					{
						if (colnames)
						{
							duckdb_libpgquery::PGList	   *namelist;

							/* extract appropriate subset of column list */
							namelist = list_copy_tail(rte->eref->colnames,
													  atts_done);
							namelist = list_truncate(namelist,
													 rtfunc->funccolcount);
							*colnames = list_concat(*colnames, namelist);
						}

						if (colvars)
						{
							duckdb_libpgquery::PGListCell   *l1;
							duckdb_libpgquery::PGListCell   *l2;
							duckdb_libpgquery::PGListCell   *l3;
							int			attnum = atts_done;

							forthree(l1, rtfunc->funccoltypes,
									 l2, rtfunc->funccoltypmods,
									 l3, rtfunc->funccolcollations)
							{
								Oid			attrtype = lfirst_oid(l1);
								int32		attrtypmod = lfirst_int(l2);
								Oid			attrcollation = lfirst_oid(l3);
								duckdb_libpgquery::PGVar		   *varnode;

								attnum++;
								varnode = duckdb_libpgquery::makeVar(rtindex,
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
						duckdb_libpgquery::PGVar      *varnode = duckdb_libpgquery::makeVar(rtindex,
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
		case duckdb_libpgquery::PG_RTE_VALUES:
			{
				/* Values RTE */
				duckdb_libpgquery::PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				int32	   *coltypmods;
				duckdb_libpgquery::PGListCell   *lcv;
				duckdb_libpgquery::PGListCell   *lcc;

				/*
				 * It's okay to extract column types from the expressions in
				 * the first row, since all rows will have been coerced to the
				 * same types.  Their typmods might not be the same though, so
				 * we potentially need to examine all rows to compute those.
				 * Column collations are pre-computed in values_collations.
				 */
				if (colvars)
					coltypmods = getValuesTypmods(rte);
				else
					coltypmods = NULL;

				varattno = 0;
				forboth(lcv, (List *) linitial(rte->values_lists),
						lcc, rte->values_collations)
				{
					duckdb_libpgquery::PGNode	   *col = (duckdb_libpgquery::PGNode *) lfirst(lcv);
					Oid			colcollation = lfirst_oid(lcc);

					varattno++;
					if (colnames)
					{
						/* Assume there is one alias per column */
						char	   *label = strVal(lfirst(aliasp_item));

						*colnames = lappend(*colnames,
											duckdb_libpgquery::makeString(duckdb_libpgquery::pstrdup(label)));
						aliasp_item = lnext(aliasp_item);
					}

					if (colvars)
					{
						duckdb_libpgquery::PGVar		   *varnode;

						varnode = duckdb_libpgquery::makeVar(rtindex, varattno,
										  duckdb_libpgquery::exprType(col),
										  coltypmods[varattno - 1],
										  colcollation,
										  sublevels_up);
						varnode->location = location;
						*colvars = lappend(*colvars, varnode);
					}
				}
				if (coltypmods)
					pfree(coltypmods);
			}
			break;
		case duckdb_libpgquery::PG_RTE_JOIN:
			{
				/* Join RTE */
				duckdb_libpgquery::PGListCell   *colname;
				duckdb_libpgquery::PGListCell   *aliasvar;

				Assert(list_length(rte->eref->colnames) == list_length(rte->joinaliasvars));

				varattno = 0;
				forboth(colname, rte->eref->colnames, aliasvar, rte->joinaliasvars)
				{
					duckdb_libpgquery::PGNode	   *avar = (duckdb_libpgquery::PGNode *) lfirst(aliasvar);

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
													duckdb_libpgquery::makeString(duckdb_libpgquery::pstrdup("")));
							if (colvars)
							{
								/*
								 * Can't use join's column type here (it might
								 * be dropped!); but it doesn't really matter
								 * what type the Const claims to be.
								 */
								*colvars = lappend(*colvars,
												   duckdb_libpgquery::makeNullConst(INT4OID, -1,
																 InvalidOid));
							}
						}
						continue;
					}

					if (colnames)
					{
						char	   *label = strVal(lfirst(colname));

						*colnames = lappend(*colnames,
											duckdb_libpgquery::makeString(duckdb_libpgquery::pstrdup(label)));
					}

					if (colvars)
					{
						duckdb_libpgquery::PGVar		   *varnode;

						varnode = duckdb_libpgquery::makeVar(rtindex, varattno,
										  duckdb_libpgquery::exprType(avar),
										  duckdb_libpgquery::exprTypmod(avar),
										  duckdb_libpgquery::exprCollation(avar),
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}
				}
			}
			break;
		case duckdb_libpgquery::PG_RTE_CTE:
			{
				duckdb_libpgquery::PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				duckdb_libpgquery::PGListCell   *lct;
				duckdb_libpgquery::PGListCell   *lcm;
				duckdb_libpgquery::PGListCell   *lcc;

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

						*colnames = lappend(*colnames, duckdb_libpgquery::makeString(duckdb_libpgquery::pstrdup(label)));
						aliasp_item = lnext(aliasp_item);
					}

					if (colvars)
					{
						duckdb_libpgquery::PGVar		   *varnode;

						varnode = duckdb_libpgquery::makeVar(rtindex, varattno,
										  coltype, coltypmod, colcoll,
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}
				}
			}
			break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
	}
};

void
RelationParser::checkNameSpaceConflicts(PGParseState *pstate, duckdb_libpgquery::PGList *namespace1,
						duckdb_libpgquery::PGList *namespace2)
{
	duckdb_libpgquery::PGListCell   *l1;

	foreach(l1, namespace1)
	{
		PGParseNamespaceItem *nsitem1 = (PGParseNamespaceItem *) lfirst(l1);
		duckdb_libpgquery::PGRangeTblEntry *rte1 = nsitem1->p_rte;
		const char *aliasname1 = rte1->eref->aliasname;
		duckdb_libpgquery::PGListCell   *l2;

		if (!nsitem1->p_rel_visible)
			continue;

		foreach(l2, namespace2)
		{
			PGParseNamespaceItem *nsitem2 = (PGParseNamespaceItem *) lfirst(l2);
			duckdb_libpgquery::PGRangeTblEntry *rte2 = nsitem2->p_rte;

			if (!nsitem2->p_rel_visible)
				continue;
			if (strcmp(rte2->eref->aliasname, aliasname1) != 0)
				continue;		/* definitely no conflict */
			if (rte1->rtekind == duckdb_libpgquery::PGRTEKind::PG_RTE_RELATION && rte1->alias == NULL &&
				rte2->rtekind == duckdb_libpgquery::PGRTEKind::PG_RTE_RELATION && rte2->alias == NULL &&
				rte1->relid != rte2->relid)
				continue;		/* no conflict per SQL rule */
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("table name \"%s\" specified more than once",
							aliasname1)));
		}
	}
};

duckdb_libpgquery::PGRangeTblEntry *
RelationParser::addRangeTableEntryForCTE(PGParseState *pstate,
						 duckdb_libpgquery::PGCommonTableExpr *cte,
						 Index levelsup,
						 duckdb_libpgquery::PGRangeVar *rv,
						 bool inFromCl)
{
	duckdb_libpgquery::PGRangeTblEntry *rte = makeNode(duckdb_libpgquery::PGRangeTblEntry);
	duckdb_libpgquery::PGAlias	   *alias = rv->alias;
	char	   *refname = alias ? alias->aliasname : cte->ctename;
	duckdb_libpgquery::PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	duckdb_libpgquery::PGListCell   *lc;

	rte->rtekind = RTE_CTE;
	rte->ctename = cte->ctename;
	rte->ctelevelsup = levelsup;

	/* Self-reference if and only if CTE's parse analysis isn't completed */
	rte->self_reference = !IsA(cte->ctequery, duckdb_libpgquery::PGQuery);
	Assert(cte->cterecursive || !rte->self_reference);
	/* Bump the CTE's refcount if this isn't a self-reference */
	if (!rte->self_reference)
		cte->cterefcount++;

	/*
	 * We throw error if the CTE is INSERT/UPDATE/DELETE without RETURNING.
	 * This won't get checked in case of a self-reference, but that's OK
	 * because data-modifying CTEs aren't allowed to be recursive anyhow.
	 */
	if (IsA(cte->ctequery, duckdb_libpgquery::PGQuery))
	{
		duckdb_libpgquery::PGQuery *ctequery = (duckdb_libpgquery::PGQuery *) cte->ctequery;

		if (ctequery->commandType != duckdb_libpgquery::PGCmdType::PG_CMD_SELECT &&
			ctequery->returningList == NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("WITH query \"%s\" does not have a RETURNING clause",
						cte->ctename),
					 parser_errposition(pstate, rv->location)));
	}

	rte->ctecoltypes = cte->ctecoltypes;
	rte->ctecoltypmods = cte->ctecoltypmods;
	rte->ctecolcollations = cte->ctecolcollations;

	rte->alias = alias;
	if (alias)
		eref = copyObject(alias);
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
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("table \"%s\" has %d columns available but %d columns specified",
						refname, varattno, numaliases)));

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

duckdb_libpgquery::PGCommonTableExpr *
RelationParser::scanNameSpaceForCTE(PGParseState *pstate, const char *refname,
					Index *ctelevelsup)
{
	Index		levelsup;

	for (levelsup = 0;
		 pstate != NULL;
		 pstate = pstate->parentParseState, levelsup++)
	{
		duckdb_libpgquery::PGListCell   *lc;

		foreach(lc, pstate->p_ctenamespace)
		{
			duckdb_libpgquery::PGCommonTableExpr *cte = (duckdb_libpgquery::PGCommonTableExpr *) lfirst(lc);

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
RelationParser::get_rte_attribute_type(duckdb_libpgquery::PGRangeTblEntry *rte, PGAttrNumber attnum,
					   Oid *vartype, int32 *vartypmod, Oid *varcollid)
{
	switch (rte->rtekind)
	{
		case duckdb_libpgquery::PG_RTE_RELATION:
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
							(errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   NameStr(att_tup->attname),
						   get_rel_name(rte->relid))));
				*vartype = att_tup->atttypid;
				*vartypmod = att_tup->atttypmod;
				*varcollid = att_tup->attcollation;
				ReleaseSysCache(tp);
			}
			break;
		case duckdb_libpgquery::PG_RTE_SUBQUERY:
			{
				/* Subselect RTE --- get type info from subselect's tlist */
				duckdb_libpgquery::PGTargetEntry *te = get_tle_by_resno(rte->subquery->targetList,
												   attnum);

				if (te == NULL || te->resjunk)
					elog(ERROR, "subquery %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				*vartype = duckdb_libpgquery::exprType((duckdb_libpgquery::PGNode *) te->expr);
				*vartypmod = duckdb_libpgquery::exprTypmod((duckdb_libpgquery::PGNode *) te->expr);
				*varcollid = duckdb_libpgquery::exprCollation((duckdb_libpgquery::PGNode *) te->expr);
			}
			break;
		case duckdb_libpgquery::PG_RTE_TABLEFUNCTION:
		case duckdb_libpgquery::PG_RTE_FUNCTION:
			{
				/* Function RTE */
				duckdb_libpgquery::PGListCell   *lc;
				int			atts_done = 0;

				/* Identify which function covers the requested column */
				foreach(lc, rte->functions)
				{
					duckdb_libpgquery::PGRangeTblFunction *rtfunc = (duckdb_libpgquery::PGRangeTblFunction *) lfirst(lc);

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
								ereport(ERROR,
										(errcode(ERRCODE_UNDEFINED_COLUMN),
										 errmsg("column \"%s\" of relation \"%s\" does not exist",
												NameStr(att_tup->attname),
												rte->eref->aliasname)));
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
							elog(ERROR, "function in FROM has unsupported return type");
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
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column %d of relation \"%s\" does not exist",
								attnum,
								rte->eref->aliasname)));
			}
			break;
		case duckdb_libpgquery::PG_RTE_VALUES:
			{
				/*
				 * Values RTE --- we can get type info from first sublist, but
				 * typmod may require scanning all sublists, and collation is
				 * stored separately.  Using getValuesTypmods() is overkill,
				 * but this path is taken so seldom for VALUES that it's not
				 * worth writing extra code.
				 */
				duckdb_libpgquery::PGList	   *collist = (List *) linitial(rte->values_lists);
				duckdb_libpgquery::PGNode	   *col;
				int32	   *coltypmods = getValuesTypmods(rte);

				if (attnum < 1 || attnum > list_length(collist))
					elog(ERROR, "values list %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				col = (duckdb_libpgquery::PGNode *) list_nth(collist, attnum - 1);
				*vartype = duckdb_libpgquery::exprType(col);
				*vartypmod = coltypmods[attnum - 1];
				*varcollid = list_nth_oid(rte->values_collations, attnum - 1);
				pfree(coltypmods);
			}
			break;
		case duckdb_libpgquery::PG_RTE_JOIN:
			{
				/*
				 * Join RTE --- get type info from join RTE's alias variable
				 */
				duckdb_libpgquery::PGNode	   *aliasvar;

				Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
				aliasvar = (duckdb_libpgquery::PGNode *) list_nth(rte->joinaliasvars, attnum - 1);
				Assert(aliasvar != NULL);
				*vartype = exprType(aliasvar);
				*vartypmod = exprTypmod(aliasvar);
				*varcollid = exprCollation(aliasvar);
			}
			break;
		case duckdb_libpgquery::PG_RTE_CTE:
			{
				/* CTE RTE --- get type info from lists in the RTE */
				Assert(attnum > 0 && attnum <= list_length(rte->ctecoltypes));
				*vartype = list_nth_oid(rte->ctecoltypes, attnum - 1);
				*vartypmod = list_nth_int(rte->ctecoltypmods, attnum - 1);
				*varcollid = list_nth_oid(rte->ctecolcollations, attnum - 1);
			}
			break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
	}
}

duckdb_libpgquery::PGVar *
RelationParser::make_var(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int attrno, int location)
{
	duckdb_libpgquery::PGVar		   *result;
	int			vnum,
				sublevels_up;
	Oid			vartypeid;
	int32		type_mod;
	Oid			varcollid;

	vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);
	get_rte_attribute_type(rte, attrno, &vartypeid, &type_mod, &varcollid);
	result = duckdb_libpgquery::makeVar(vnum, attrno, vartypeid, type_mod, varcollid, sublevels_up);
	result->location = location;
	return result;
}

duckdb_libpgquery::PGNode *
RelationParser::scanRTEForColumn(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, char *colname,
				 int location)
{
	duckdb_libpgquery::PGNode	   *result = NULL;
	int			attnum = 0;
	duckdb_libpgquery::PGVar		   *var;
	duckdb_libpgquery::PGListCell   *c;

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
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_COLUMN),
						 errmsg("column reference \"%s\" is ambiguous",
								colname),
						 parser_errposition(pstate, location)));
			var = make_var(pstate, rte, attnum, location);
			/* Require read access to the column */
			markVarForSelectPriv(pstate, var, rte);
			result = (duckdb_libpgquery::PGNode *) var;
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
	if (rte->rtekind == duckdb_libpgquery::PG_RTE_RELATION)
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
		if (pstate->p_expr_kind == EXPR_KIND_CHECK_CONSTRAINT &&
			attnum < InvalidAttrNumber /* && attnum != TableOidAttributeNumber */)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("system column \"%s\" reference in check constraint is invalid",
							colname),
					 parser_errposition(pstate, location)));

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
				result = (duckdb_libpgquery::PGNode *) var;
			}
		}
	}

	return result;
};

duckdb_libpgquery::PGRangeTblEntry *
RelationParser::addRangeTableEntry(PGParseState *pstate,
				   duckdb_libpgquery::PGRangeVar *relation,
				   duckdb_libpgquery::PGAlias *alias,
				   bool inh,
				   bool inFromCl)
{
	duckdb_libpgquery::PGRangeTblEntry *rte = makeNode(duckdb_libpgquery::PGRangeTblEntry);
	char	   *refname = alias ? alias->aliasname : relation->relname;
	LOCKMODE	lockmode = AccessShareLock;
	duckdb_libpgquery::PGLockingClause *locking;
	Relation	rel;
	ParseCallbackState pcbstate;

	rte->alias = alias;
	rte->rtekind = duckdb_libpgquery::PG_RTE_RELATION;

	/*
	 * CDB: lock promotion around the locking clause is a little different
	 * from postgres to allow for required lock promotion for distributed
	 * AO tables.
	 * select for update should lock the whole table, we do it here.
	 */
	locking = getLockedRefname(pstate, refname);
	if (locking)
	{
		if (locking->strength >= LCS_FORNOKEYUPDATE)
		{
			Oid relid;

			relid = RangeVarGetRelid(relation, lockmode, false);

			rel = try_heap_open(relid, NoLock, true);
			if (!rel)
				elog(ERROR, "open relation(%u) fail", relid);

			if (rel->rd_rel->relkind == RELKIND_MATVIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
								errmsg("cannot lock rows in materialized view \"%s\"",
									   RelationGetRelationName(rel))));

			lockmode = IsSystemRelation(rel) ? RowExclusiveLock : ExclusiveLock;
			heap_close(rel, NoLock);
		}
		else
		{
			lockmode = RowShareLock;
		}
	}

	/*
	 * Get the rel's OID.  This access also ensures that we have an up-to-date
	 * relcache entry for the rel.  Since this is typically the first access
	 * to a rel in a statement, be careful to get the right access level
	 * depending on whether we're doing SELECT FOR UPDATE/SHARE.
	 */
	setup_parser_errposition_callback(&pcbstate, pstate, relation->location);
	rel = parserOpenTable(pstate, relation, lockmode, NULL);
	cancel_parser_errposition_callback(&pcbstate);
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;

	/*
	 * Build the list of effective column names using user-supplied aliases
	 * and/or actual column names.
	 */
	rte->eref = makeAlias(refname, NIL);
	buildRelationAliases(rel->rd_att, alias, rte->eref);

	/*
	 * Drop the rel refcount, but keep the access lock till end of transaction
	 * so that the table can't be deleted or have its schema modified
	 * underneath us.
	 */
	heap_close(rel, NoLock);

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

duckdb_libpgquery::PGRangeTblEntry *
RelationParser::addRangeTableEntryForSubquery(PGParseState *pstate,
							  duckdb_libpgquery::PGQuery *subquery,
							  duckdb_libpgquery::PGAlias *alias,
							  bool lateral,
							  bool inFromCl)
{
	duckdb_libpgquery::PGRangeTblEntry *rte = makeNode(duckdb_libpgquery::PGRangeTblEntry);
	char	   *refname = alias->aliasname;
	duckdb_libpgquery::PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	duckdb_libpgquery::PGListCell   *tlistitem;

	rte->rtekind = duckdb_libpgquery::PG_RTE_SUBQUERY;
	rte->relid = InvalidOid;
	rte->subquery = subquery;
	rte->alias = alias;

	eref = reinterpret_cast<duckdb_libpgquery::PGAlias*>(copyObject(alias));
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	varattno = 0;
	foreach(tlistitem, subquery->targetList)
	{
		duckdb_libpgquery::PGTargetEntry *te = (duckdb_libpgquery::PGTargetEntry *) lfirst(tlistitem);

		if (te->resjunk)
			continue;
		varattno++;
		Assert(varattno == te->resno);
		if (varattno > numaliases)
		{
			char	   *attrname;

			attrname = duckdb_libpgquery::pstrdup(te->resname);
			eref->colnames = lappend(eref->colnames, duckdb_libpgquery::makeString(attrname));
		}
	}
	if (varattno < numaliases)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("table \"%s\" has %d columns available but %d columns specified",
						refname, varattno, numaliases)));

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

void RelationParser::markRTEForSelectPriv(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
					 int rtindex, PGAttrNumber col)
{
	if (rte == NULL)
		rte = rt_fetch(rtindex, pstate->p_rtable);

	if (rte->rtekind == duckdb_libpgquery::PG_RTE_RELATION)
	{
		/* Make sure the rel as a whole is marked for SELECT access */
		rte->requiredPerms |= duckdb_libpgquery::PG_ACL_SELECT;
		/* Must offset the attnum to fit in a bitmapset */
		rte->selectedCols = duckdb_libpgquery::bms_add_member(rte->selectedCols,
								   col - FirstLowInvalidHeapAttributeNumber);
	}
	else if (rte->rtekind == duckdb_libpgquery::PG_RTE_JOIN)
	{
		if (col == InvalidAttrNumber)
		{
			/*
			 * A whole-row reference to a join has to be treated as whole-row
			 * references to the two inputs.
			 */
			duckdb_libpgquery::PGJoinExpr   *j;

			if (rtindex > 0 && rtindex <= list_length(pstate->p_joinexprs))
				j = (duckdb_libpgquery::PGJoinExpr *) list_nth(pstate->p_joinexprs, rtindex - 1);
			else
				j = NULL;
			if (j == NULL)
				elog(ERROR, "could not find JoinExpr for whole-row reference");
			Assert(IsA(j, duckdb_libpgquery::PGJoinExpr));

			/* Note: we can't see FromExpr here */
			if (IsA(j->larg, duckdb_libpgquery::PGRangeTblRef))
			{
				int			varno = ((duckdb_libpgquery::PGRangeTblRef *) j->larg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else if (IsA(j->larg, duckdb_libpgquery::PGJoinExpr))
			{
				int			varno = ((duckdb_libpgquery::PGJoinExpr *) j->larg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(j->larg));
			if (IsA(j->rarg, duckdb_libpgquery::PGRangeTblRef))
			{
				int			varno = ((duckdb_libpgquery::PGRangeTblRef *) j->rarg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else if (IsA(j->rarg, duckdb_libpgquery::PGJoinExpr))
			{
				int			varno = ((duckdb_libpgquery::PGJoinExpr *) j->rarg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(j->rarg));
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
			Var		   *aliasvar;

			Assert(col > 0 && col <= list_length(rte->joinaliasvars));
			aliasvar = (Var *) list_nth(rte->joinaliasvars, col - 1);
			aliasvar = (Var *) strip_implicit_coercions((Node *) aliasvar);
			if (aliasvar && IsA(aliasvar, Var))
				markVarForSelectPriv(pstate, aliasvar, NULL);
		}
	}
};

void 
RelationParser::markVarForSelectPriv(PGParseState *pstate, duckdb_libpgquery::PGVar *var, duckdb_libpgquery::PGRangeTblEntry *rte)
{
	Index		lv;

	Assert(IsA(var, duckdb_libpgquery::PGVar));
	/* Find the appropriate pstate if it's an uplevel Var */
	for (lv = 0; lv < var->varlevelsup; lv++)
		pstate = pstate->parentParseState;
	markRTEForSelectPriv(pstate, rte, var->varno, var->varattno);
}

duckdb_libpgquery::PGNode *
RelationParser::colNameToVar(PGParseState *pstate, char *colname, bool localonly,
			 int location)
{
	duckdb_libpgquery::PGNode	   *result = NULL;
	PGParseState *orig_pstate = pstate;

	while (pstate != NULL)
	{
		duckdb_libpgquery::PGListCell   *l;

		foreach(l, pstate->p_namespace)
		{
			PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
			duckdb_libpgquery::PGRangeTblEntry *rte = nsitem->p_rte;
			duckdb_libpgquery::PGNode	   *newresult;

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
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							 errmsg("column reference \"%s\" is ambiguous",
									colname),
							 parser_errposition(pstate, location)));
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

duckdb_libpgquery::PGRangeTblEntry *
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

duckdb_libpgquery::PGCommonTableExpr *
RelationParser::GetCTEForRTE(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int rtelevelsup)
{
	Index		levelsup;
	duckdb_libpgquery::PGListCell   *lc;

	/* Determine RTE's levelsup if caller didn't know it */
	if (rtelevelsup < 0)
		(void) RTERangeTablePosn(pstate, rte, &rtelevelsup);

	Assert(rte->rtekind == duckdb_libpgquery::PG_RTE_CTE);
	levelsup = rte->ctelevelsup + rtelevelsup;
	while (levelsup-- > 0)
	{
		pstate = pstate->parentParseState;
		if (!pstate)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}
	foreach(lc, pstate->p_ctenamespace)
	{
		duckdb_libpgquery::PGCommonTableExpr *cte = (duckdb_libpgquery::PGCommonTableExpr *) lfirst(lc);

		if (strcmp(cte->ctename, rte->ctename) == 0)
			return cte;
	}
	/* shouldn't happen */
	elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
	return NULL;				/* keep compiler quiet */
};

}