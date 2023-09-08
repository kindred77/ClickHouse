#include <Interpreters/orcaopt/RelationParser.h>

#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/TypeParser.h>
//#include <Interpreters/orcaopt/ENRParser.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>
#include <Interpreters/orcaopt/provider/FunctionProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

// RelationParser::RelationParser(const ContextPtr& context_) : context(context_)
// {
// 	coerce_parser = std::make_shared<CoerceParser>(context);
// 	node_parser = std::make_shared<NodeParser>(context);
// 	type_parser = std::make_shared<TypeParser>(context);
// };

PGCommonTableExpr *
RelationParser::scanNameSpaceForCTE(PGParseState *pstate, const char *refname,
					PGIndex *ctelevelsup)
{
	PGIndex		levelsup;

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

PGRangeTblEntry *
RelationParser::addRangeTableEntryForCTE(PGParseState *pstate,
						 PGCommonTableExpr *cte,
						 PGIndex levelsup,
						 PGRangeVar *rv,
						 bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	PGAlias	   *alias = rv->alias;
	char	   *refname = alias ? alias->aliasname : cte->ctename;
	PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	ListCell   *lc;

	Assert(pstate != NULL)

	rte->rtekind = PG_RTE_CTE;
	rte->ctename = cte->ctename;
	rte->ctelevelsup = levelsup;

	/* Self-reference if and only if CTE's parse analysis isn't completed */
	rte->self_reference = !IsA(cte->ctequery, PGQuery);
	Assert(cte->cterecursive || !rte->self_reference)
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
		PGQuery	   *ctequery = (PGQuery *) cte->ctequery;

		if (ctequery->commandType != PG_CMD_SELECT &&
			ctequery->returningList == NIL)
		{
			parser_errposition(pstate, rv->location);
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("WITH query \"%s\" does not have a RETURNING clause",
							cte->ctename)));
		}
	}

	rte->coltypes = cte->ctecoltypes;
	rte->coltypmods = cte->ctecoltypmods;
	rte->colcollations = cte->ctecolcollations;

	rte->alias = alias;
	if (alias)
		eref = (PGAlias *)copyObject(alias);
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
				(errcode(PG_ERRCODE_INVALID_COLUMN_REFERENCE),
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

	// rte->requiredPerms = 0;
	// rte->checkAsUser = InvalidOid;
	// rte->selectedCols = NULL;
	// rte->insertedCols = NULL;
	// rte->updatedCols = NULL;
	// rte->extraUpdatedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

void
RelationParser::buildRelationAliases(PGTupleDescPtr tupdesc, PGAlias *alias, PGAlias *eref)
{
    int maxattrs = tupdesc->natts;
    PGListCell * aliaslc;
    int numaliases;
    int varattno;
    int numdropped = 0;

    Assert(eref->colnames == NIL)

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

    for (varattno = 0; varattno < maxattrs; varattno++)
    {
        PGAttrPtr attr = tupdesc->attrs[varattno];
        PGValue * attrname;

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
            attrname = (PGValue *)lfirst(aliaslc);
            aliaslc = lnext(aliaslc);
            alias->colnames = lappend(alias->colnames, attrname);
        }
        else
        {
            attrname = makeString(pstrdup(attr->attname.c_str()));
            /* we're done with the alias if any */
        }

        eref->colnames = lappend(eref->colnames, attrname);
    }

    /* Too many user-supplied aliases? */
    if (aliaslc)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_INVALID_COLUMN_REFERENCE),
             errmsg("table \"%s\" has %d columns available but %d columns specified", eref->aliasname, maxattrs - numdropped, numaliases)));
};

PGRangeTblEntry *
RelationParser::searchRangeTableForRel(PGParseState *pstate, PGRangeVar *relation)
{
    const char * refname = relation->relname;
    PGOid relId = InvalidOid;
    PGCommonTableExpr * cte = NULL;
    PGIndex ctelevelsup = 0;
    PGIndex levelsup;

    /*
	 * If it's an unqualified name, check for possible CTE matches. A CTE
	 * hides any real relation matches.  If no CTE, look for a matching
	 * relation.
	 *
	 * NB: It's not critical that RangeVarGetRelid return the correct answer
	 * here in the face of concurrent DDL.  If it doesn't, the worst case
	 * scenario is a less-clear error message.  Also, the tables involved in
	 * the query are already locked, which reduces the number of cases in
	 * which surprising behavior can occur.  So we do the name lookup
	 * unlocked.
	 */
    if (!relation->schemaname)
        cte = scanNameSpaceForCTE(pstate, refname, &ctelevelsup);
    if (!cte)
        relId = RelationProvider::RangeVarGetRelidExtended(relation, NoLock, true, false, NULL, NULL);

    /* Now look for RTEs matching either the relation/CTE or the alias */
    for (levelsup = 0; pstate != NULL; pstate = pstate->parentParseState, levelsup++)
    {
        PGListCell * l;

        foreach (l, pstate->p_rtable)
        {
            PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(l);

            if (rte->rtekind == PG_RTE_RELATION && OidIsValid(relId) && rte->relid == relId)
                return rte;
            if (rte->rtekind == PG_RTE_CTE && cte != NULL && rte->ctelevelsup + levelsup == ctelevelsup && strcmp(rte->ctename, refname) == 0)
                return rte;

            if (rte->eref != NULL && rte->eref->aliasname != NULL && strcmp(rte->eref->aliasname, refname) == 0)
                return rte;
        }
    }
    return NULL;
};

void
RelationParser::errorMissingRTE(PGParseState *pstate, PGRangeVar *relation)
{
	PGRangeTblEntry *rte;
	int			sublevels_up;
	const char *badAlias = NULL;

	/*
	 * Check to see if there are any potential matches in the query's
	 * rangetable.  (Note: cases involving a bad schema name in the RangeVar
	 * will throw error immediately here.  That seems OK.)
	 */
	rte = searchRangeTableForRel(pstate, relation);

	/*
	 * If we found a match that has an alias and the alias is visible in the
	 * namespace, then the problem is probably use of the relation's real name
	 * instead of its alias, ie "SELECT foo.* FROM foo f". This mistake is
	 * common enough to justify a specific hint.
	 *
	 * If we found a match that doesn't meet those criteria, assume the
	 * problem is illegal use of a relation outside its scope, as in the
	 * MySQL-ism "SELECT ... FROM a, b LEFT JOIN c ON (a.x = c.y)".
	 */
	if (rte && rte->alias &&
		strcmp(rte->eref->aliasname, relation->relname) != 0 &&
		refnameRangeTblEntry(pstate, NULL, rte->eref->aliasname,
							 relation->location,
							 &sublevels_up) == rte)
		badAlias = rte->eref->aliasname;

	if (rte)
	{
		parser_errposition(pstate, relation->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_UNDEFINED_TABLE),
				 errmsg("invalid reference to FROM-clause entry for table \"%s\"",
						relation->relname),
				 (badAlias ?
				  errmsg("Perhaps you meant to reference the table alias \"%s\".",
						  badAlias) :
				  errmsg("There is an entry for table \"%s\", but it cannot be referenced from this part of the query.",
						  rte->eref->aliasname))));
	}
	else
	{
		parser_errposition(pstate, relation->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_UNDEFINED_TABLE),
				 errmsg("missing FROM-clause entry for table \"%s\"",
						relation->relname)));
	}
};

void
RelationParser::errorMissingColumn(PGParseState *pstate,
				   const char *relname, char *colname, int location)
{
    PGRangeTblEntry * rte;

    /*
	 * If relname was given, just play dumb and report it.  (In practice, a
	 * bad qualification name should end up at errorMissingRTE, not here, so
	 * no need to work hard on this case.)
	 */
    if (relname)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_UNDEFINED_COLUMN),
             errmsg("column %s.%s does not exist", relname, colname),
             parser_errposition(pstate, location)));

    /*
	 * Otherwise, search the entire rtable looking for possible matches.  If
	 * we find one, emit a hint about it.
	 *
	 * TODO: improve this code (and also errorMissingRTE) to mention using
	 * LATERAL if appropriate.
	 */
    rte = searchRangeTableForCol(pstate, colname, location);

    ereport(
        ERROR,
        (errcode(PG_ERRCODE_UNDEFINED_COLUMN),
         errmsg("column \"%s\" does not exist", colname),
         rte ? errmsg(
             "There is a column named \"%s\" in table \"%s\", but it cannot be referenced from this part of the query.",
             colname,
             rte->eref->aliasname)
             : 0,
         parser_errposition(pstate, location)));
};

PGRangeTblEntry * RelationParser::searchRangeTableForCol(PGParseState * pstate, char * colname, int location)
{
	PGParseState *orig_pstate = pstate;

	while (pstate != NULL)
	{
		ListCell   *l;

		foreach(l, pstate->p_rtable)
		{
			PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(l);

			if (scanRTEForColumn(orig_pstate, rte, colname, location))
				return rte;
		}

		pstate = pstate->parentParseState;
	}
	return NULL;
};

void
RelationParser::expandTupleDesc(PGTupleDescPtr tupdesc, PGAlias *eref, int count, int offset,
				int rtindex, int sublevels_up,
				int location, bool include_dropped,
				PGList **colnames, PGList **colvars)
{
    PGListCell * aliascell = list_head(eref->colnames);
    int varattno;

    if (colnames)
    {
        int i;

        for (i = 0; i < offset; i++)
        {
            if (aliascell)
                aliascell = lnext(aliascell);
        }
    }

    Assert(count <= tupdesc->natts)
    for (varattno = 0; varattno < count; varattno++)
    {
        PGAttrPtr attr = tupdesc->attrs[varattno];

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

					int16_t typLen;
                    bool typByVal;
                    TypeProvider::get_typlenbyval(INT4OID, &typLen, &typByVal);
                    *colvars = lappend(*colvars, makeNullConst(typLen, typByVal, INT4OID, -1, InvalidOid));
                }
            }
            if (aliascell)
                aliascell = lnext(aliascell);
            continue;
        }

        if (colnames)
        {
            const char * label;

            if (aliascell)
            {
                label = strVal(lfirst(aliascell));
                aliascell = lnext(aliascell);
            }
            else
            {
                /* If we run out of aliases, use the underlying name */
                label = attr->attname.c_str();
            }
            *colnames = lappend(*colnames, makeString(pstrdup(label)));
        }

        if (colvars)
        {
            PGVar * varnode;

            varnode = makeVar(rtindex, varattno + offset + 1, attr->atttypid, attr->atttypmod, attr->attcollation, sublevels_up);
            varnode->location = location;

            *colvars = lappend(*colvars, varnode);
        }
    }
};

PGList *
RelationParser::expandRelAttrs(PGParseState *pstate, PGRangeTblEntry *rte,
			   int rtindex, int sublevels_up, int location)
{
	PGList	   *names,
			   *vars;
	PGListCell   *name,
			   *var;
	PGList	   *te_list = NIL;

	expandRTE(rte, rtindex, sublevels_up, location, false,
			  &names, &vars);

	/*
	 * Require read access to the table.  This is normally redundant with the
	 * markVarForSelectPriv calls below, but not if the table has zero
	 * columns.
	 */
	// rte->requiredPerms |= ACL_SELECT;

	forboth(name, names, var, vars)
	{
		char	   *label = strVal(lfirst(name));
		PGVar		   *varnode = (PGVar *) lfirst(var);
		PGTargetEntry *te;

		te = makeTargetEntry((PGExpr *) varnode,
							 (PGAttrNumber) pstate->p_next_resno++,
							 label,
							 false);
		te_list = lappend(te_list, te);

		/* Require read access to each column */
		markVarForSelectPriv(pstate, varnode, rte);
	}

	Assert(name == NULL && var == NULL)	/* lists not the same length? */

	return te_list;
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

PGCommonTableExpr *
RelationParser::GetCTEForRTE(PGParseState *pstate, PGRangeTblEntry *rte, int rtelevelsup)
{
	PGIndex		levelsup;
	PGListCell   *lc;

	/* Determine RTE's levelsup if caller didn't know it */
	if (rtelevelsup < 0)
		(void) RTERangeTablePosn(pstate, rte, &rtelevelsup);

	Assert(rte->rtekind == PG_RTE_CTE)
	levelsup = rte->ctelevelsup + rtelevelsup;
	while (levelsup-- > 0)
	{
		pstate = pstate->parentParseState;
		if (!pstate)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}
	foreach(lc, pstate->p_ctenamespace)
	{
		PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);

		if (strcmp(cte->ctename, rte->ctename) == 0)
			return cte;
	}
	/* shouldn't happen */
	elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
	return NULL;				/* keep compiler quiet */
};

int
RelationParser::RTERangeTablePosn(PGParseState *pstate, PGRangeTblEntry *rte, int *sublevels_up)
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

void
RelationParser::expandRelation(PGOid relid, PGAlias *eref, int rtindex, int sublevels_up,
			   int location, bool include_dropped,
			   PGList **colnames, PGList **colvars)
{
	/* Get the tupledesc and turn it over to expandTupleDesc */
	PGRelationPtr rel = RelationProvider::relation_open(relid, AccessShareLock);
	if (!rel)
	{
		elog(ERROR, "Can not get table. oid: %u", relid);
		return;
	}
	expandTupleDesc(rel->rd_att, eref, rel->rd_att->natts, 0,
					rtindex, sublevels_up,
					location, include_dropped,
					colnames, colvars);
	RelationProvider::relation_close(rel, AccessShareLock);
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
					Assert(varattno == te->resno)

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
		// case RTE_TABLEFUNCTION:
		case PG_RTE_FUNCTION:
			{
				/* Function RTE */
				int			atts_done = 0;
				PGListCell   *lc;

				foreach(lc, rte->functions)
				{
					PGRangeTblFunction *rtfunc = (PGRangeTblFunction *) lfirst(lc);
					TypeFuncClass functypclass;
					PGOid			funcrettype;
					PGTupleDescPtr tupdesc = nullptr;

					functypclass = TypeProvider::get_expr_result_type(rtfunc->funcexpr,
														&funcrettype,
														tupdesc);
					if (functypclass == TYPEFUNC_COMPOSITE /* ||
						functypclass == TYPEFUNC_COMPOSITE_DOMAIN */)
					{
						/* Composite data type, e.g. a table's row type */
						Assert(tupdesc)
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
								PGOid			attrtype = lfirst_oid(l1);
								int32		attrtypmod = lfirst_int(l2);
								PGOid			attrcollation = lfirst_oid(l3);
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
						PGVar		   *varnode = makeVar(rtindex,
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
		case PG_RTE_JOIN:
			{
				/* Join RTE */
				ListCell   *colname;
				ListCell   *aliasvar;

				Assert(list_length(rte->eref->colnames) == list_length(rte->joinaliasvars))

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
								int16_t typLen;
                                bool typByVal;
                                TypeProvider::get_typlenbyval(INT4OID, &typLen, &typByVal);
								*colvars = lappend(*colvars,
												   makeNullConst(typLen, typByVal, INT4OID, -1,
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
		case PG_RTE_TABLEFUNC:
		case PG_RTE_VALUES:
		case PG_RTE_CTE:
		case RTE_NAMEDTUPLESTORE:
			{
				/* Tablefunc, Values, CTE, or ENR RTE */
				PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				PGListCell   *lct;
				PGListCell   *lcm;
				PGListCell   *lcc;

				varattno = 0;
				forthree(lct, rte->coltypes,
						 lcm, rte->coltypmods,
						 lcc, rte->colcollations)
				{
					PGOid			coltype = lfirst_oid(lct);
					int32		coltypmod = lfirst_int(lcm);
					PGOid			colcoll = lfirst_oid(lcc);

					varattno++;

					if (colnames)
					{
						/* Assume there is one alias per output column */
						if (OidIsValid(coltype))
						{
							char	   *label = strVal(lfirst(aliasp_item));

							*colnames = lappend(*colnames,
												makeString(pstrdup(label)));
						}
						else if (include_dropped)
							*colnames = lappend(*colnames,
												makeString(pstrdup("")));

						aliasp_item = lnext(aliasp_item);
					}

					if (colvars)
					{
						if (OidIsValid(coltype))
						{
							PGVar		   *varnode;

							varnode = makeVar(rtindex, varattno,
											  coltype, coltypmod, colcoll,
											  sublevels_up);
							varnode->location = location;

							*colvars = lappend(*colvars, varnode);
						}
						else if (include_dropped)
						{
							/*
							 * It doesn't really matter what type the Const
							 * claims to be.
							 */
							int16_t typLen;
                            bool typByVal;
                            TypeProvider::get_typlenbyval(INT4OID, &typLen, &typByVal);
							*colvars = lappend(*colvars,
											   makeNullConst(typLen, typByVal, INT4OID, -1,
															 InvalidOid));
						}
					}
				}
			}
			break;
		// case RTE_RESULT:
		// 	/* These expose no columns, so nothing to do */
		// 	break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
	}
};

bool RelationParser::isFutureCTE(PGParseState * pstate, const char * refname)
{
    for (; pstate != NULL; pstate = pstate->parentParseState)
    {
        PGListCell * lc;

        foreach (lc, pstate->p_future_ctes)
        {
            PGCommonTableExpr * cte = (PGCommonTableExpr *)lfirst(lc);

            if (strcmp(cte->ctename, refname) == 0)
                    return true;
        }
    }
    return false;
};

PGRelationPtr RelationParser::parserOpenTable(PGParseState * pstate, const PGRangeVar * relation, int lockmode, bool * lockUpgraded)
{
    //ParseCallbackState pcbstate;

    //setup_parser_errposition_callback(&pcbstate, pstate, relation->location);

    /* Look up the appropriate relation using namespace search */
    PGOid relid = RelationProvider::RangeVarGetRelidExtended(relation, NoLock, true, false, NULL, NULL);
    
	if (relid == InvalidOid)
	{
		elog(ERROR, "Table not found, %s.", relation->relname);
		return nullptr;
	}
    /*
	 * CdbTryOpenRelation might return NULL (for example, if the table
	 * is dropped by another transaction). Every time we invoke function
	 * CdbTryOpenRelation, we should check if the return value is NULL.
	 */
    auto rel = RelationProvider::heap_open(relid, lockmode);

    if (!rel)
    {
        if (relation->schemaname)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s.%s\" does not exist", relation->schemaname, relation->relname)));
        else
        {
            /*
			 * An unqualified name might have been meant as a reference to
			 * some not-yet-in-scope CTE.  The bare "does not exist" message
			 * has proven remarkably unhelpful for figuring out such problems,
			 * so we take pains to offer a specific hint.
			 */
            if (isFutureCTE(pstate, relation->relname))
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_UNDEFINED_TABLE),
                     errmsg("relation \"%s\" does not exist", relation->relname),
                     errdetail(
                         "There is a WITH item named \"%s\", but it cannot be referenced from this part of the query.", relation->relname),
                     errhint("Use WITH RECURSIVE, or re-order the WITH items to remove forward references.")));
            else
                ereport(ERROR, (errcode(PG_ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" does not exist", relation->relname)));
        }
    }

    //cancel_parser_errposition_callback(&pcbstate);
    return rel;
};

PGRangeTblEntry *
RelationParser::addRangeTableEntry(PGParseState *pstate,
				   PGRangeVar *relation,
				   PGAlias *alias,
				   bool inh,
				   bool inFromCl)
{
    PGRangeTblEntry * rte = makeNode(PGRangeTblEntry);
    char * refname = alias ? alias->aliasname : relation->relname;
    LOCKMODE lockmode = AccessShareLock;
    PGLockingClause * locking;
    PGRelationPtr rel = nullptr;
    PGParseCallbackState pcbstate;

    rte->alias = alias;
    rte->rtekind = PG_RTE_RELATION;

    /*
	 * CDB: lock promotion around the locking clause is a little different
	 * from postgres to allow for required lock promotion for distributed
	 * AO tables.
	 * select for update should lock the whole table, we do it here.
	 */
    locking = getLockedRefname(pstate, refname);
    if (locking)
    {
        if (locking->strength >= PG_LCS_FORNOKEYUPDATE)
        {
            PGOid relid;

            relid = RelationProvider::RangeVarGetRelidExtended(relation, lockmode, false, false, NULL, NULL);

            rel = RelationProvider::try_heap_open(relid, NoLock, true);
            if (!rel)
                    elog(ERROR, "open relation(%u) fail", relid);

            if (rel->rd_rel->relkind == PG_RELKIND_MATVIEW)
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("cannot lock rows in materialized view \"%s\"", RelationGetRelationName(rel))));

            lockmode = RelationProvider::IsSystemRelation(rel) ? RowExclusiveLock : ExclusiveLock;
            RelationProvider::relation_close(rel, NoLock);
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
    rte->relid = rel->rd_id;
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
    RelationProvider::relation_close(rel, NoLock);

    /*
	 * Set flags and access permissions.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 */
    rte->lateral = false;
    rte->inh = inh;
    rte->inFromCl = inFromCl;

	//TODO kindred
    // rte->requiredPerms = PG_ACL_SELECT;
    // rte->checkAsUser = InvalidOid; /* not set-uid by default, either */
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
	ListCell   *tlistitem;

	rte->rtekind = PG_RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->alias = alias;

	eref = (PGAlias *)copyObject(alias);
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	varattno = 0;
	foreach(tlistitem, subquery->targetList)
	{
		PGTargetEntry *te = (PGTargetEntry *) lfirst(tlistitem);

		if (te->resjunk)
			continue;
		varattno++;
		Assert(varattno == te->resno)
		if (varattno > numaliases)
		{
			char	   *attrname;

			attrname = pstrdup(te->resname);
			eref->colnames = lappend(eref->colnames, makeString(attrname));
		}
	}
	if (varattno < numaliases)
		ereport(ERROR,
				(errcode(PG_ERRCODE_INVALID_COLUMN_REFERENCE),
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

	// rte->requiredPerms = 0;
	// rte->checkAsUser = InvalidOid;
	// rte->selectedCols = NULL;
	// rte->insertedCols = NULL;
	// rte->updatedCols = NULL;
	// rte->extraUpdatedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
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
		ListCell   *l2;

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
			if (rte1->rtekind == PG_RTE_RELATION && rte1->alias == NULL &&
				rte2->rtekind == PG_RTE_RELATION && rte2->alias == NULL &&
				rte1->relid != rte2->relid)
				continue;		/* no conflict per SQL rule */
			ereport(ERROR,
					(errcode(PG_ERRCODE_DUPLICATE_ALIAS),
					 errmsg("table name \"%s\" specified more than once",
							aliasname1)));
		}
	}
};

PGRangeTblEntry *
RelationParser::GetRTEByRangeTablePosn(PGParseState *pstate,
					   int varno,
					   int sublevels_up)
{
	while (sublevels_up-- > 0)
	{
		pstate = pstate->parentParseState;
		Assert(pstate != NULL)
	}
	Assert(varno > 0 && varno <= list_length(pstate->p_rtable))
	return rt_fetch(varno, pstate->p_rtable);
};

PGLockingClause *
RelationParser::getLockedRefname(PGParseState *pstate, const char *refname)
{
	PGListCell   *l;

	/*
	 * If we are in a subquery specified as locked FOR UPDATE/SHARE from
	 * parent level, then act as though there's a generic FOR UPDATE here.
	 */
	if (pstate->p_lockclause_from_parent)
		return pstate->p_lockclause_from_parent;

	foreach(l, pstate->p_locking_clause)
	{
		PGLockingClause *lc = (PGLockingClause *) lfirst(l);

		if (lc->lockedRels == NIL)
		{
			/* all tables used in query */
			return lc;
		}
		else
		{
			/* just the named tables */
			ListCell   *l2;

			foreach(l2, lc->lockedRels)
			{
				PGRangeVar   *thisrel = (PGRangeVar *) lfirst(l2);

				if (strcmp(refname, thisrel->relname) == 0)
					return lc;
			}
		}
	}

	return NULL;
};

PGRangeTblEntry *
RelationParser::scanNameSpaceForRelid(PGParseState *pstate, PGOid relid, int location)
{
	PGRangeTblEntry *result = NULL;
	PGListCell   *l;

	foreach(l, pstate->p_namespace)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
		PGRangeTblEntry *rte = nsitem->p_rte;

		/* Ignore columns-only items */
		if (!nsitem->p_rel_visible)
			continue;
		/* If not inside LATERAL, ignore lateral-only items */
		if (nsitem->p_lateral_only && !pstate->p_lateral_active)
			continue;

		/* yes, the test for alias == NULL should be there... */
		if (rte->rtekind == PG_RTE_RELATION &&
			rte->relid == relid &&
			rte->alias == NULL)
		{
			if (result)
			{
				parser_errposition(pstate, location);
				ereport(ERROR,
						(errcode(PG_ERRCODE_AMBIGUOUS_ALIAS),
						 errmsg("table reference %u is ambiguous",
								relid)));
			}
			check_lateral_ref_ok(pstate, nsitem, location);
			result = rte;
		}
	}
	return result;
};

void
RelationParser::check_lateral_ref_ok(PGParseState *pstate, PGParseNamespaceItem *nsitem,
					 int location)
{
	if (nsitem->p_lateral_only && !nsitem->p_lateral_ok)
	{
		/* SQL:2008 demands this be an error, not an invisible item */
		PGRangeTblEntry *rte = nsitem->p_rte;
		char	   *refname = rte->eref->aliasname;

		parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("invalid reference to FROM-clause entry for table \"%s\"",
						refname),
				 (rte == pstate->p_target_rangetblentry) ?
				 errmsg("There is an entry for table \"%s\", but it cannot be referenced from this part of the query.",
						 refname) :
				 errdetail("The combining JOIN type must be INNER or LEFT for a LATERAL reference.")));
	}
};

PGRangeTblEntry *
RelationParser::scanNameSpaceForRefname(PGParseState *pstate, const char *refname, int location)
{
	PGRangeTblEntry *result = NULL;
	PGListCell   *l;

	foreach(l, pstate->p_namespace)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
		PGRangeTblEntry *rte = nsitem->p_rte;

		/* Ignore columns-only items */
		if (!nsitem->p_rel_visible)
			continue;
		/* If not inside LATERAL, ignore lateral-only items */
		if (nsitem->p_lateral_only && !pstate->p_lateral_active)
			continue;

		if (strcmp(rte->eref->aliasname, refname) == 0)
		{
			if (result)
			{
				parser_errposition(pstate, location);
				ereport(ERROR,
						(errcode(PG_ERRCODE_AMBIGUOUS_ALIAS),
						 errmsg("table reference \"%s\" is ambiguous",
								refname)));
			}
			check_lateral_ref_ok(pstate, nsitem, location);
			result = rte;
		}
	}
	return result;
};

PGRangeTblEntry *
RelationParser::refnameRangeTblEntry(PGParseState *pstate,
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up)
{
	PGOid			relId = InvalidOid;

	if (sublevels_up)
		*sublevels_up = 0;

	if (schemaname != NULL)
	{
		PGOid			namespaceId;

		/*
		 * We can use LookupNamespaceNoError() here because we are only
		 * interested in finding existing RTEs.  Checking USAGE permission on
		 * the schema is unnecessary since it would have already been checked
		 * when the RTE was made.  Furthermore, we want to report "RTE not
		 * found", not "no permissions for schema", if the name happens to
		 * match a schema name the user hasn't got access to.
		 */
		namespaceId = RelationProvider::LookupNamespaceNoError(schemaname);
		if (!OidIsValid(namespaceId))
			return NULL;
		relId = RelationProvider::get_relname_relid(refname, namespaceId);
		if (!OidIsValid(relId))
			return NULL;
	}

	while (pstate != NULL)
	{
		PGRangeTblEntry *result;

		if (OidIsValid(relId))
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

	Assert(pstate != NULL)

	/*
	 * Fail if join has too many columns --- we must be able to reference any
	 * of the columns with an AttrNumber.
	 */
	if (list_length(aliasvars) > MaxAttrNumber)
		ereport(ERROR,
				(errcode(PG_ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("joins can have at most %d columns",
						MaxAttrNumber)));

	rte->rtekind = PG_RTE_JOIN;
	rte->relid = InvalidOid;
	rte->subquery = NULL;
	rte->jointype = jointype;
	rte->joinaliasvars = aliasvars;
	rte->alias = alias;

	eref = alias ? (PGAlias *)copyObject(alias) : makeAlias("unnamed_join", NIL);
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
	// rte->insertedCols = NULL;
	// rte->updatedCols = NULL;
	// rte->extraUpdatedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

void
RelationParser::markRTEForSelectPriv(PGParseState *pstate, PGRangeTblEntry *rte,
					 int rtindex, PGAttrNumber col)
{
	if (rte == NULL)
		rte = rt_fetch(rtindex, pstate->p_rtable);

	if (rte->rtekind == PG_RTE_RELATION)
	{
		// /* Make sure the rel as a whole is marked for SELECT access */
		// rte->requiredPerms |= PG_ACL_SELECT;
		// /* Must offset the attnum to fit in a bitmapset */
		// rte->selectedCols = bms_add_member(rte->selectedCols,
		// 								   col - FirstLowInvalidHeapAttributeNumber);
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
				j = list_nth_node(PGJoinExpr, pstate->p_joinexprs, rtindex - 1);
			else
				j = NULL;
			if (j == NULL)
				elog(ERROR, "could not find JoinExpr for whole-row reference");

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
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(j->larg));
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
			PGVar		   *aliasvar;

			Assert(col > 0 && col <= list_length(rte->joinaliasvars))
			aliasvar = (PGVar *) list_nth(rte->joinaliasvars, col - 1);
			aliasvar = (PGVar *) strip_implicit_coercions((PGNode *) aliasvar);
			if (aliasvar && IsA(aliasvar, PGVar))
				markVarForSelectPriv(pstate, aliasvar, NULL);
		}
	}
	/* other RTE types don't require privilege marking */
};

void
RelationParser::markVarForSelectPriv(PGParseState *pstate, PGVar *var, PGRangeTblEntry *rte)
{
	PGIndex		lv;

	Assert(IsA(var, PGVar))
	/* Find the appropriate pstate if it's an uplevel Var */
	for (lv = 0; lv < var->varlevelsup; lv++)
		pstate = pstate->parentParseState;
	markRTEForSelectPriv(pstate, rte, var->varno, var->varattno);
};

// int RelationParser::specialAttNum(const char * attname)
// {
//     PGAttrPtr sysatt;

//     sysatt = RelationProvider::SystemAttributeByName(attname, true /* "oid" will be accepted */);
//     if (sysatt != NULL)
//         return sysatt->attnum;
//     return InvalidAttrNumber;
// };

PGNode * RelationParser::scanRTEForColumn(PGParseState * pstate,
		PGRangeTblEntry * rte, const char * colname, int location)
{
    PGNode * result = NULL;
    int attnum = 0;
    PGVar * var;
    PGListCell * c;

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
    foreach (c, rte->eref->colnames)
    {
        attnum++;
        if (strcmp(strVal(lfirst(c)), colname) == 0)
        {
            if (result)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_AMBIGUOUS_COLUMN),
                     errmsg("column reference \"%s\" is ambiguous", colname),
                     parser_errposition(pstate, location)));
            var = NodeParser::make_var(pstate, rte, attnum, location);
            /* Require read access to the column */
            markVarForSelectPriv(pstate, var, rte);
            result = (PGNode *)var;
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
	// TODO kindred
    // if (rte->rtekind == PG_RTE_RELATION)
    // {
    //     /* In GPDB, system columns like gp_segment_id, ctid, xmin/xmax seem to be
	// 	 * ambiguous for replicated table, replica in each segment has different
	// 	 * value of those columns, between sessions, different replicas are choosen
	// 	 * to provide data, so it's weird for users to see different system columns
	// 	 * between sessions. So for replicated table, we don't expose system columns
	// 	 * unless it's GP_ROLE_UTILITY for debug purpose.
	// 	 */
	// 	//TODO kindred
    //     if (RelationProvider::PGPolicyIsReplicated(RelationProvider::PGPolicyFetch(rte->relid)) /* && Gp_role != GP_ROLE_UTILITY */)
    //         return result;

    //     /* quick check to see if name could be a system column */
    //     attnum = specialAttNum(colname);

    //     /* In constraint check, no system column is allowed except tableOid */
    //     /*
	// 	 * In GPDB, tableoid is not allowed either, because we've removed
	// 	 * HeapTupleData.t_tableOid field.
	// 	 * GPDB_94_MERGE_FIXME: Could we make that work somehow? Resurrect
	// 	 * t_tableOid, maybe? I think we'd need it for logical decoding as well.
	// 	 */
    //     if (pstate->p_expr_kind == EXPR_KIND_CHECK_CONSTRAINT && attnum < InvalidAttrNumber /* && attnum != TableOidAttributeNumber */)
    //         ereport(
    //             ERROR,
    //             (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
    //              errmsg("system column \"%s\" reference in check constraint is invalid", colname),
    //              parser_errposition(pstate, location)));

    //     if (attnum != InvalidAttrNumber)
    //     {
    //         /*
	// 		 * Now check to see if column actually is defined.  Because of
	// 		 * an ancient oversight in DefineQueryRewrite, it's possible that
	// 		 * pg_attribute contains entries for system columns for a view,
	// 		 * even though views should not have such --- so we also check
	// 		 * the relkind.  This kluge will not be needed in 9.3 and later.
	// 		 */
    //         if (RelationProvider::AttrExistsInRel(rte->relid, attnum)
    //             && RelationProvider::get_rel_relkind(rte->relid) != PG_RELKIND_VIEW)
    //         {
    //             var = NodeParser::make_var(pstate, rte, attnum, location);
    //             /* Require read access to the column */
    //             markVarForSelectPriv(pstate, var, rte);
    //             result = (PGNode *)var;
    //         }
    //     }
    // }

    return result;
};

PGNode *
RelationParser::colNameToVar(PGParseState *pstate, const char *colname, bool localonly,
			 int location)
{
    PGNode * result = NULL;
    PGParseState * orig_pstate = pstate;

    while (pstate != NULL)
    {
        ListCell * l;

        foreach (l, pstate->p_namespace)
        {
            PGParseNamespaceItem * nsitem = (PGParseNamespaceItem *)lfirst(l);
            PGRangeTblEntry * rte = nsitem->p_rte;
            PGNode * newresult;

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
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_AMBIGUOUS_COLUMN),
                         errmsg("column reference \"%s\" is ambiguous", colname),
                         parser_errposition(pstate, location)));
                check_lateral_ref_ok(pstate, nsitem, location);
                result = newresult;
            }
        }

        if (result != NULL || localonly)
            break; /* found, or don't want to look at parent */

        pstate = pstate->parentParseState;
    }

    return result;
};

bool
RelationParser::isSimplyUpdatableRelation(PGOid relid, bool noerror)
{
    bool return_value = true;

    if (!OidIsValid(relid))
    {
        if (!noerror)
            ereport(ERROR, (errcode(PG_ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid oid: %d is not simply updatable", relid)));
        return false;
    }

    PGRelationPtr rel = RelationProvider::relation_open(relid, AccessShareLock);

    do
    {
        /*
		 * This should match the error message in rewriteManip.c,
		 * so that you get the same error as in PostgreSQL.
		 */
        if (rel->rd_rel->relkind == PG_RELKIND_VIEW)
        {
            if (!noerror)
                ereport(ERROR, (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("WHERE CURRENT OF on a view is not implemented")));
            return_value = false;
            break;
        }

        if (rel->rd_rel->relkind != PG_RELKIND_RELATION)
        {
            if (!noerror)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not simply updatable", RelationGetRelationName(rel))));
            return_value = false;
            break;
        }

        if (rel->rd_rel->relstorage != PG_RELSTORAGE_HEAP)
        {
            if (!noerror)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not simply updatable", RelationGetRelationName(rel))));
            return_value = false;
            break;
        }

        /*
		 * A row in replicated table cannot be identified by (ctid + gp_segment_id)
		 * in all replicas, for each row replica, the gp_segment_id is different,
		 * the ctid is also not guaranteed to be the same, so it's not simply
		 * updateable for CURRENT OF.
		 */
        if (RelationProvider::PGPolicyIsReplicated(rel->rd_cdbpolicy))
        {
            if (!noerror)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not simply updatable", RelationGetRelationName(rel))));
            return_value = false;
            break;
        }
    } while (0);

    RelationProvider::relation_close(rel, NoLock);
    return return_value;
};

int32 * RelationParser::getValuesTypmods(PGRangeTblEntry * rte)
{
    int32 * coltypmods;
    PGList * firstrow;
    int ncolumns, nvalid, i;
    PGListCell * lc;

    Assert(rte->values_lists != NIL)
    firstrow = (PGList *)linitial(rte->values_lists);
    ncolumns = list_length(firstrow);
    coltypmods = (int32 *)palloc(ncolumns * sizeof(int32));
    nvalid = 0;

    /* Collect the typmods from the first VALUES row */
    i = 0;
    foreach (lc, firstrow)
    {
        PGNode * col = (PGNode *)lfirst(lc);

        coltypmods[i] = exprTypmod(col);
        if (coltypmods[i] >= 0)
            nvalid++;
        i++;
    }

    /*
	 * Scan remaining rows; as soon as we have a non-matching typmod for a
	 * column, reset that typmod to -1.  We can bail out early if all typmods
	 * become -1.
	 */
    if (nvalid > 0)
    {
        for_each_cell(lc, lnext(list_head(rte->values_lists)))
        {
            PGList * thisrow = (PGList *)lfirst(lc);
            PGListCell * lc2;

            Assert(list_length(thisrow) == ncolumns)
            i = 0;
            foreach (lc2, thisrow)
            {
                PGNode * col = (PGNode *)lfirst(lc2);

                if (coltypmods[i] >= 0 && coltypmods[i] != exprTypmod(col))
                {
                    coltypmods[i] = -1;
                    nvalid--;
                }
                i++;
            }

            if (nvalid <= 0)
                break;
        }
    }

    return coltypmods;
};

void
RelationParser::get_rte_attribute_type(PGRangeTblEntry *rte, PGAttrNumber attnum,
					   PGOid *vartype, int32 *vartypmod, PGOid *varcollid)
{
    switch (rte->rtekind)
    {
        case PG_RTE_RELATION: {
            /* Plain relation RTE --- get the attribute's type info */
			PGAttrPtr tp = RelationProvider::get_att_by_reloid_attnum(rte->relid, attnum);
            if (tp == NULL) /* shouldn't happen */
                elog(ERROR, "cache lookup failed for attribute %d of relation %u", attnum, rte->relid);

            /*
				 * If dropped column, pretend it ain't there.  See notes in
				 * scanRTEForColumn.
				 */
            if (tp->attisdropped)
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_UNDEFINED_COLUMN),
                         errmsg("column \"%s\" of relation \"%s\" does not exist", tp->attname.c_str(), RelationProvider::get_rel_name(rte->relid).c_str())));
            *vartype = tp->atttypid;
            *vartypmod = tp->atttypmod;
            *varcollid = tp->attcollation;
        }
        break;
        case PG_RTE_SUBQUERY: {
            /* Subselect RTE --- get type info from subselect's tlist */
            PGTargetEntry * te = get_tle_by_resno(rte->subquery->targetList, attnum);

            if (te == NULL || te->resjunk)
                    elog(ERROR, "subquery %s does not have attribute %d", rte->eref->aliasname, attnum);
            *vartype = exprType((PGNode *)te->expr);
            *vartypmod = exprTypmod((PGNode *)te->expr);
            *varcollid = exprCollation((PGNode *)te->expr);
        }
        break;
		//TODO kindred
        // case PG_RTE_TABLEFUNCTION:
        case PG_RTE_FUNCTION: {
            /* Function RTE */
            PGListCell * lc;
            int atts_done = 0;

            /* Identify which function covers the requested column */
            foreach (lc, rte->functions)
            {
                    PGRangeTblFunction * rtfunc = (PGRangeTblFunction *)lfirst(lc);

                    if (attnum > atts_done && attnum <= atts_done + rtfunc->funccolcount)
                    {
                        PGOid funcrettype;
                        PGTupleDescPtr tupdesc = nullptr;

                        attnum -= atts_done; /* now relative to this func */
                        TypeFuncClass functypclass = TypeProvider::get_expr_result_type(rtfunc->funcexpr, &funcrettype, tupdesc);

                        if (functypclass == TYPEFUNC_COMPOSITE)
                        {
                            /* Composite data type, e.g. a table's row type */

                            Assert(tupdesc)
                            Assert(attnum <= tupdesc->natts)
                            PGAttrPtr att_tup = tupdesc->attrs[attnum - 1];

                            /*
							 * If dropped column, pretend it ain't there.  See
							 * notes in scanRTEForColumn.
							 */
                            if (att_tup->attisdropped)
                                ereport(
                                    ERROR,
                                    (errcode(PG_ERRCODE_UNDEFINED_COLUMN),
                                     errmsg(
                                         "column \"%s\" of relation \"%s\" does not exist",
                                         att_tup->attname.c_str(),
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
                            *vartype = list_nth_oid(rtfunc->funccoltypes, attnum - 1);
                            *vartypmod = list_nth_int(rtfunc->funccoltypmods, attnum - 1);
                            *varcollid = list_nth_oid(rtfunc->funccolcollations, attnum - 1);
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
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_UNDEFINED_COLUMN), errmsg("column %d of relation \"%s\" does not exist", attnum, rte->eref->aliasname)));
        }
        break;
        case PG_RTE_VALUES: {
            /*
				 * Values RTE --- we can get type info from first sublist, but
				 * typmod may require scanning all sublists, and collation is
				 * stored separately.  Using getValuesTypmods() is overkill,
				 * but this path is taken so seldom for VALUES that it's not
				 * worth writing extra code.
				 */
            PGList * collist = (PGList *)linitial(rte->values_lists);
            PGNode * col;
            int32 * coltypmods = getValuesTypmods(rte);

            if (attnum < 1 || attnum > list_length(collist))
                    elog(ERROR, "values list %s does not have attribute %d", rte->eref->aliasname, attnum);
            col = (PGNode *)list_nth(collist, attnum - 1);
            *vartype = exprType(col);
            *vartypmod = coltypmods[attnum - 1];
			//TODO kindred
            //*varcollid = list_nth_oid(rte->values_collations, attnum - 1);
            pfree(coltypmods);
        }
        break;
        case PG_RTE_JOIN: {
            /*
				 * Join RTE --- get type info from join RTE's alias variable
				 */
            PGNode * aliasvar;

            Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars))
            aliasvar = (PGNode *)list_nth(rte->joinaliasvars, attnum - 1);
            Assert(aliasvar != NULL)
            *vartype = exprType(aliasvar);
            *vartypmod = exprTypmod(aliasvar);
            *varcollid = exprCollation(aliasvar);
        }
        break;
        case PG_RTE_CTE: {
			//TODO kindred
            // /* CTE RTE --- get type info from lists in the RTE */
            // Assert(attnum > 0 && attnum <= list_length(rte->ctecoltypes))
            // *vartype = list_nth_oid(rte->ctecoltypes, attnum - 1);
            // *vartypmod = list_nth_int(rte->ctecoltypmods, attnum - 1);
            // *varcollid = list_nth_oid(rte->ctecolcollations, attnum - 1);
        }
        break;
        default:
            elog(ERROR, "unrecognized RTE kind: %d", (int)rte->rtekind);
    }
};

String RelationParser::chooseScalarFunctionAlias(PGNode * funcexpr, char * funcname, PGAlias * alias, int nfuncs)
{
    /*
	 * If the expression is a simple function call, and the function has a
	 * single OUT parameter that is named, use the parameter's name.
	 */
    if (funcexpr && IsA(funcexpr, PGFuncExpr))
    {
        auto pname = FunctionProvider::get_func_result_name(((PGFuncExpr *)funcexpr)->funcid);
        if (pname != "")
            return pname;
    }

    /*
	 * If there's just one function in the RTE, and the user gave an RTE alias
	 * name, use that name.  (This makes FROM func() AS foo use "foo" as the
	 * column name as well as the table alias.)
	 */
    if (nfuncs == 1 && alias)
        return alias->aliasname;

    /*
	 * Otherwise use the function name.
	 */
    return String(funcname);
};

PGRangeTblEntry * RelationParser::addRangeTableEntryForFunction(
        PGParseState * pstate,
        PGList * funcnames,
        PGList * funcexprs,
        PGList * coldeflists,
        PGRangeFunction * rangefunc,
        bool lateral,
        bool inFromCl)
{
    PGRangeTblEntry * rte = makeNode(PGRangeTblEntry);
    //Oid funcDescribe = InvalidOid;
    PGAlias * alias = rangefunc->alias;
    PGAlias * eref;
    char * aliasname;
    int nfuncs = list_length(funcexprs);
    std::vector<PGTupleDescPtr> functupdescs(nfuncs);
    PGTupleDescPtr tupdesc;
    PGListCell *lc1, *lc2, *lc3;
    int i;
    int j;
    int funcno;
    int natts, totalatts;

    rte->rtekind = PG_RTE_FUNCTION;
    rte->relid = InvalidOid;
    rte->subquery = NULL;
    rte->functions = NIL; /* we'll fill this list below */
    rte->funcordinality = rangefunc->ordinality;
    rte->alias = alias;

    /*
	 * Choose the RTE alias name.  We default to using the first function's
	 * name even when there's more than one; which is maybe arguable but beats
	 * using something constant like "table".
	 */
    if (alias)
        aliasname = alias->aliasname;
    else
        aliasname = (char *)linitial(funcnames);

    eref = makeAlias(aliasname, NIL);
    rte->eref = eref;

    /* Process each function ... */
    //functupdescs = (TupleDesc *)palloc(nfuncs * sizeof(TupleDesc));

    totalatts = 0;
    funcno = 0;
    forthree(lc1, funcexprs, lc2, funcnames, lc3, coldeflists)
    {
        PGNode * funcexpr = (PGNode *)lfirst(lc1);
        char * funcname = (char *)lfirst(lc2);
        PGList * coldeflist = (PGList *)lfirst(lc3);
        PGRangeTblFunction * rtfunc = makeNode(PGRangeTblFunction);
        TypeFuncClass functypclass;
        PGOid funcrettype;

        /* Initialize RangeTblFunction node */
        rtfunc->funcexpr = funcexpr;
        rtfunc->funccolnames = NIL;
        rtfunc->funccoltypes = NIL;
        rtfunc->funccoltypmods = NIL;
        rtfunc->funccolcollations = NIL;
        rtfunc->funcparams = NULL; /* not set until planning */

        /*
		 * If the function has TABLE value expressions in its arguments then it must
		 * be planned as a TableFunctionScan instead of a normal FunctionScan.  We
		 * mark this here because this is where we know that the function is being
		 * used as a RangeTableEntry.
		 */
        if (funcexpr && IsA(funcexpr, PGFuncExpr))
        {
            PGFuncExpr * func = (PGFuncExpr *)funcexpr;

            if (func->args && IsA(func->args, PGList))
            {
                    ListCell * arg;

                    foreach (arg, (PGList *)func->args)
                    {
						//TODO kindred
                        // PGNode * n = (PGNode *)lfirst(arg);
                        // if (IsA(n, TableValueExpr))
                        // {
                        //     TableValueExpr * input = (TableValueExpr *)n;

                        //     /* 
						//  * Currently only support single TABLE value expression.
						//  *
						//  * Note: this shouldn't be possible given that we don't
						//  * allow it at function creation so the function parser
						//  * should have already errored due to type mismatch.
						//  */
                        //     Assert(IsA(input->subquery, Query));
                        //     if (rte->subquery != NULL)
                        //         ereport(
                        //             ERROR,
                        //             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        //              errmsg("functions over multiple TABLE value "
                        //                     "expressions not supported")));

                        //     /*
						//  * Convert RTE to a TableFunctionScan over the specified
						//  * input 
						//  */
                        //     rte->rtekind = RTE_TABLEFUNCTION;
                        //     rte->subquery = (Query *)input->subquery;

                        //     /* 
						//  * Mark function as a table function so that the second pass
						//  * check, parseCheckTableFunctions(), can correctly detect
						//  * that it is a valid TABLE value expression.
						//  */
                        //     func->is_tablefunc = true;

                        //     /*
						//  * We do not break from the loop here because we want to
						//  * keep looping to guard against multiple TableValueExpr
						//  * arguments.
						//  */
                        // }
                    }
            }
        }

        /*
		 * Now determine if the function returns a simple or composite type.
		 */
        functypclass = TypeProvider::get_expr_result_type(funcexpr, &funcrettype, tupdesc);

        /*
		 * Handle dynamic type resolution for functions with DESCRIBE callbacks.
		 */
        /* GPDB_94_MERGE_FIXME: What happens if you have 'coldeflist', and a DESCRIBE callback? */
        if (functypclass == TYPEFUNC_RECORD && IsA(funcexpr, PGFuncExpr))
        {
			//TODO kindred
            // PGFuncExpr * func = (PGFuncExpr *)funcexpr;
            // Datum d;
            // int i;

			
            // Insist(TypeSupportsDescribe(funcrettype));
			// Assert(TypeSupportsDescribe(funcrettype))

			//TODO kindred
            // funcDescribe = lookupProcCallback(func->funcid, PROMETHOD_DESCRIBE);
            // if (OidIsValid(funcDescribe))
            // {
            //         FmgrInfo flinfo;
            //         FunctionCallInfoData fcinfo;

            //         /*
			// 	 * Describe functions have the signature  d(internal) => internal
			// 	 * where the parameter is the untransformed FuncExpr node and the result
			// 	 * is a tuple descriptor. Its context is RangeTblFunction which has
			// 	 * funcuserdata field to store arbitrary binary data to transport
			// 	 * to executor.
			// 	 */
            //         rtfunc->funcuserdata = NULL;
            //         fmgr_info(funcDescribe, &flinfo);
            //         InitFunctionCallInfoData(fcinfo, &flinfo, 1, InvalidOid, (Node *)rtfunc, NULL);
            //         fcinfo.arg[0] = PointerGetDatum(funcexpr);
            //         fcinfo.argnull[0] = false;

            //         d = FunctionCallInvoke(&fcinfo);
            //         if (fcinfo.isnull)
            //             elog(ERROR, "function %u returned NULL", flinfo.fn_oid);
            //         tupdesc = (TupleDesc)DatumGetPointer(d);

            //         /* 
			// 	 * Might want to improve this API so the describe method return 
			// 	 * value is somehow verifiable 
			// 	 */
            //         if (tupdesc != NULL)
            //         {
            //             functypclass = TYPEFUNC_COMPOSITE;
            //             for (i = 0; i < tupdesc->natts; i++)
            //             {
            //                 Form_pg_attribute attr = tupdesc->attrs[i];

            //                 rtfunc->funccolnames = lappend(rtfunc->funccolnames, makeString(pstrdup(NameStr(attr->attname))));
            //                 rtfunc->funccoltypes = lappend_oid(rtfunc->funccoltypes, attr->atttypid);
            //                 rtfunc->funccoltypmods = lappend_int(rtfunc->funccoltypmods, attr->atttypmod);
            //                 rtfunc->funccolcollations = lappend_oid(rtfunc->funccolcollations, attr->attcollation);
            //             }
            //         }
            // }
        }

        /*
		 * A coldeflist is required if the function returns RECORD and hasn't
		 * got a predetermined record type, and is prohibited otherwise.
		 */
        if (coldeflist != NIL)
        {
            if (functypclass != TYPEFUNC_RECORD)
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_SYNTAX_ERROR),
                         errmsg("a column definition list is only allowed for functions returning \"record\""),
                         parser_errposition(pstate, exprLocation((PGNode *)coldeflist))));
        }
        else
        {
            if (functypclass == TYPEFUNC_RECORD)
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_SYNTAX_ERROR),
                         errmsg("a column definition list is required for functions returning \"record\""),
                         parser_errposition(pstate, exprLocation(funcexpr))));
        }

        if (functypclass == TYPEFUNC_COMPOSITE)
        {
            /* Composite data type, e.g. a table's row type */
            Assert(tupdesc)
        }
        else if (functypclass == TYPEFUNC_SCALAR)
        {
            /* Base data type, i.e. scalar */
            tupdesc = PGCreateTemplateTupleDesc(1, false);
            TypeProvider::PGTupleDescInitEntry(tupdesc, (PGAttrNumber)1, chooseScalarFunctionAlias(funcexpr, funcname, alias, nfuncs), funcrettype, -1, 0);
        }
        else if (functypclass == TYPEFUNC_RECORD)
        {
            ListCell * col;

            /*
			 * Use the column definition list to construct a tupdesc and fill
			 * in the RangeTblFunction's lists.
			 */
            tupdesc = PGCreateTemplateTupleDesc(list_length(coldeflist), false);
            i = 1;
            foreach (col, coldeflist)
            {
                    PGColumnDef * n = (PGColumnDef *)lfirst(col);
                    char * attrname;
                    PGOid attrtype;
                    int32 attrtypmod;
					//TODO kindred
                    //Oid attrcollation;

                    attrname = n->colname;
                    if (n->typeName->setof)
                        ereport(
                            ERROR,
                            (errcode(PG_ERRCODE_INVALID_TABLE_DEFINITION),
                             errmsg("column \"%s\" cannot be declared SETOF", attrname),
                             parser_errposition(pstate, n->location)));
                    TypeParser::typenameTypeIdAndMod(pstate, n->typeName, &attrtype, &attrtypmod);
                    //attrcollation = GetColumnDefCollation(pstate, n, attrtype);
                    TypeProvider::PGTupleDescInitEntry(tupdesc, (PGAttrNumber)i, attrname, attrtype, attrtypmod, 0);
                    //TupleDescInitEntryCollation(tupdesc, (PGAttrNumber)i, attrcollation);
                    rtfunc->funccolnames = lappend(rtfunc->funccolnames, makeString(pstrdup(attrname)));
                    rtfunc->funccoltypes = lappend_oid(rtfunc->funccoltypes, attrtype);
                    rtfunc->funccoltypmods = lappend_int(rtfunc->funccoltypmods, attrtypmod);
                    //rtfunc->funccolcollations = lappend_oid(rtfunc->funccolcollations, attrcollation);

                    i++;
            }

            /*
			 * Ensure that the coldeflist defines a legal set of names (no
			 * duplicates) and datatypes (no pseudo-types, for instance).
			 */
			//TODO kindred
            //CheckAttributeNamesTypes(tupdesc, RELKIND_COMPOSITE_TYPE, false);
        }
        else
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                 errmsg("function \"%s\" in FROM has unsupported return type %s", funcname, TypeProvider::format_type_be(funcrettype).c_str()),
                 parser_errposition(pstate, exprLocation(funcexpr))));

        /* Finish off the RangeTblFunction and add it to the RTE's list */
        rtfunc->funccolcount = tupdesc->natts;
        rte->functions = lappend(rte->functions, rtfunc);

        /* Save the tupdesc for use below */
        functupdescs[funcno] = tupdesc;
        totalatts += tupdesc->natts;
        funcno++;
    }

    /*
	 * If there's more than one function, or we want an ordinality column, we
	 * have to produce a merged tupdesc.
	 */
    if (nfuncs > 1 || rangefunc->ordinality)
    {
        if (rangefunc->ordinality)
            totalatts++;

        /* Merge the tuple descs of each function into a composite one */
        tupdesc = PGCreateTemplateTupleDesc(totalatts, false);
        natts = 0;
        for (i = 0; i < nfuncs; i++)
        {
            for (j = 1; j <= functupdescs[i]->natts; j++)
                PGTupleDescCopyEntry(tupdesc, ++natts, functupdescs[i], j);
        }

        /* Add the ordinality column if needed */
        if (rangefunc->ordinality)
            TypeProvider::PGTupleDescInitEntry(tupdesc, (PGAttrNumber)++natts, "ordinality", INT8OID, -1, 0);

        Assert(natts == totalatts)
    }
    else
    {
        /* We can just use the single function's tupdesc as-is */
        tupdesc = functupdescs[0];
    }

    /* Use the tupdesc while assigning column aliases for the RTE */
    buildRelationAliases(tupdesc, alias, eref);

    /*
	 * Set flags and access permissions.
	 *
	 * Functions are never checked for access rights (at least, not by the RTE
	 * permissions mechanism).
	 */
    rte->lateral = lateral;
    rte->inh = false; /* never true for functions */
    rte->inFromCl = inFromCl;

	//TODO kindred
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

}
