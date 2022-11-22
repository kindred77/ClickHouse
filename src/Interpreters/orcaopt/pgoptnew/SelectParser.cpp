#include <Interpreters/orcaopt/pgoptnew/SelectParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

void
SelectParser::markTargetListOrigin(PGParseState *pstate, PGTargetEntry *tle,
					 PGVar *var, int levelsup)
{
    int			netlevelsup;
	PGRangeTblEntry *rte;
	PGAttrNumber	attnum;

	if (var == NULL || !IsA(var, PGVar))
		return;
	netlevelsup = var->varlevelsup + levelsup;
	rte = relation_parser.GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
	attnum = var->varattno;

	switch (rte->rtekind)
	{
		case PG_RTE_RELATION:
			/* It's a table or view, report it */
			tle->resorigtbl = rte->relid;
			tle->resorigcol = attnum;
			break;
		case PG_RTE_SUBQUERY:
			/* Subselect-in-FROM: copy up from the subselect */
			if (attnum != InvalidAttrNumber)
			{
				PGTargetEntry *ste = relation_parser.get_tle_by_resno(rte->subquery->targetList,
													attnum);

				if (ste == NULL || ste->resjunk)
					elog(ERROR, "subquery %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				tle->resorigtbl = ste->resorigtbl;
				tle->resorigcol = ste->resorigcol;
			}
			break;
		case PG_RTE_JOIN:
			/* Join RTE --- recursively inspect the alias variable */
			if (attnum != InvalidAttrNumber)
			{
				PGVar		   *aliasvar;

				Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
				aliasvar = (PGVar *) list_nth(rte->joinaliasvars, attnum - 1);
				/* We intentionally don't strip implicit coercions here */
				markTargetListOrigin(pstate, tle, aliasvar, netlevelsup);
			}
			break;
		// case PG_RTE_TABLEFUNCTION:
		case PG_RTE_FUNCTION:
		case PG_RTE_VALUES:
		case PG_RTE_TABLEFUNC:
		// case PG_RTE_NAMEDTUPLESTORE:
		// case PG_RTE_RESULT:
		// case PG_RTE_VOID:
			/* not a simple relation, leave it unmarked */
			break;
		case PG_RTE_CTE:

			/*
			 * CTE reference: copy up from the subquery, if possible. If the
			 * RTE is a recursive self-reference then we can't do anything
			 * because we haven't finished analyzing it yet. However, it's no
			 * big loss because we must be down inside the recursive term of a
			 * recursive CTE, and so any markings on the current targetlist
			 * are not going to affect the results anyway.
			 */
			if (attnum != InvalidAttrNumber && !rte->self_reference)
			{
				PGCommonTableExpr *cte = relation_parser.GetCTEForRTE(pstate, rte, netlevelsup);
				PGTargetEntry *ste;

				ste = relation_parser.get_tle_by_resno(GetCTETargetList(cte), attnum);
				if (ste == NULL || ste->resjunk)
					elog(ERROR, "subquery %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				tle->resorigtbl = ste->resorigtbl;
				tle->resorigcol = ste->resorigcol;
			}
			break;
	}
};

void
SelectParser::markTargetListOrigins(PGParseState *pstate, PGList *targetlist)
{
    PGListCell   *l;

	foreach(l, targetlist)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(l);

		markTargetListOrigin(pstate, tle, (PGVar *) tle->expr, 0);
	}
};

void
SelectParser::transformLockingClause(PGParseState *pstate, PGQuery *qry,
                        PGLockingClause *lc,
					    bool pushedDown)
{
	PGList	   *lockedRels = lc->lockedRels;
	PGListCell   *l;
	PGListCell   *rt;
	Index		i;
	PGLockingClause *allrels;

	CheckSelectLocking(qry, lc->strength);

	/* make a clause we can pass down to subqueries to select all rels */
	allrels = makeNode(PGLockingClause);
	allrels->lockedRels = NIL;	/* indicates all rels */
	allrels->strength = lc->strength;
	allrels->waitPolicy = lc->waitPolicy;

	if (lockedRels == NIL)
	{
		/* all regular tables used in query */
		i = 0;
		foreach(rt, qry->rtable)
		{
			PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(rt);

			++i;
			switch (rte->rtekind)
			{
				case PG_RTE_RELATION:
					if (rel_is_external_table(rte->relid))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("SELECT FOR UPDATE/SHARE cannot be applied to external tables")));

					applyLockingClause(qry, i, lc->strength, lc->waitPolicy,
									   pushedDown);
					rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
					break;
				case PG_RTE_SUBQUERY:
					applyLockingClause(qry, i, lc->strength, lc->waitPolicy,
									   pushedDown);

					/*
					 * FOR UPDATE/SHARE of subquery is propagated to all of
					 * subquery's rels, too.  We could do this later (based on
					 * the marking of the subquery RTE) but it is convenient
					 * to have local knowledge in each query level about which
					 * rels need to be opened with RowShareLock.
					 */
					transformLockingClause(pstate, rte->subquery,
										   allrels, true);
					break;
				default:
					/* ignore JOIN, SPECIAL, FUNCTION, VALUES, CTE RTEs */
					break;
			}
		}
	}
	else
	{
		/* just the named tables */
		foreach(l, lockedRels)
		{
			PGRangeVar   *thisrel = (PGRangeVar *) lfirst(l);

			/* For simplicity we insist on unqualified alias names here */
			if (thisrel->catalogname || thisrel->schemaname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				/*------
				  translator: %s is a SQL row locking clause such as FOR UPDATE */
						 errmsg("%s must specify unqualified relation names",
								LCS_asString(lc->strength)),
						 node_parser.parser_errposition(pstate, thisrel->location)));

			i = 0;
			foreach(rt, qry->rtable)
			{
				PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(rt);

				++i;
				if (strcmp(rte->eref->aliasname, thisrel->relname) == 0)
				{
					switch (rte->rtekind)
					{
						case PG_RTE_RELATION:
							if (rel_is_external_table(rte->relid))
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("SELECT FOR UPDATE/SHARE cannot be applied to external tables")));
							applyLockingClause(qry, i, lc->strength,
											   lc->waitPolicy, pushedDown);
							rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
							break;
						case PG_RTE_SUBQUERY:
							applyLockingClause(qry, i, lc->strength,
											   lc->waitPolicy, pushedDown);
							/* see comment above */
							transformLockingClause(pstate, rte->subquery,
												   allrels, true);
							break;
						case PG_RTE_JOIN:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a join",
											LCS_asString(lc->strength)),
									 node_parser.parser_errposition(pstate, thisrel->location)));
							break;
						case PG_RTE_FUNCTION:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a function",
											LCS_asString(lc->strength)),
									 node_parser.parser_errposition(pstate, thisrel->location)));
							break;
						case PG_RTE_TABLEFUNC:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a table function",
											LCS_asString(lc->strength)),
									 node_parser.parser_errposition(pstate, thisrel->location)));
							break;
						// case PG_RTE_TABLEFUNCTION:
						// 	ereport(ERROR,
						// 			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						// 			 errmsg("SELECT FOR UPDATE/SHARE cannot be applied to a table function")));
						// 	break;
						case PG_RTE_VALUES:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to VALUES",
											LCS_asString(lc->strength)),
									 node_parser.parser_errposition(pstate, thisrel->location)));
							break;
						case PG_RTE_CTE:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a WITH query",
											LCS_asString(lc->strength)),
									 node_parser.parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_NAMEDTUPLESTORE:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a named tuplestore",
											LCS_asString(lc->strength)),
									 node_parser.parser_errposition(pstate, thisrel->location)));
							break;

							/* Shouldn't be possible to see RTE_RESULT here */

						default:
							elog(ERROR, "unrecognized RTE type: %d",
								 (int) rte->rtekind);
							break;
					}
					break;		/* out of foreach loop */
				}
			}
			if (rt == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_TABLE),
				/*------
				  translator: %s is a SQL row locking clause such as FOR UPDATE */
						 errmsg("relation \"%s\" in %s clause not found in FROM clause",
								thisrel->relname,
								LCS_asString(lc->strength)),
						 node_parser.parser_errposition(pstate, thisrel->location)));
		}
	}
};

PGQuery *
SelectParser::transformSelectStmt(PGParseState *pstate, PGSelectStmt *stmt)
{
    PGQuery	   *qry = makeNode(PGQuery);
	PGNode	   *qual;
	PGListCell   *l;

	qry->commandType = PG_CMD_SELECT;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = cte_parser.transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/* Complain if we get called from someplace where INTO is not allowed */
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT ... INTO is not allowed here"),
				 node_parser.parser_errposition(pstate,
									exprLocation((PGNode *) stmt->intoClause))));

	/* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
	pstate->p_locking_clause = stmt->lockingClause;

	/* make WINDOW info available for window functions, too */
	pstate->p_windowdefs = stmt->windowClause;

	/* process the FROM clause */
	clause_parser.transformFromClause(pstate, stmt->fromClause);

	/* transform targetlist */
	qry->targetList = target_parser.transformTargetList(pstate, stmt->targetList,
										  PGParseExprKind::EXPR_KIND_SELECT_TARGET);

	/* mark column origins */
	markTargetListOrigins(pstate, qry->targetList);

	/* transform WHERE */
	qual = clause_parser.transformWhereClause(pstate, stmt->whereClause,
								PGParseExprKind::EXPR_KIND_WHERE, "WHERE");

	/* initial processing of HAVING clause is much like WHERE clause */
	qry->havingQual = clause_parser.transformWhereClause(pstate, stmt->havingClause,
										   PGParseExprKind::EXPR_KIND_HAVING, "HAVING");

	/*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
	qry->sortClause = clause_parser.transformSortClause(pstate,
										  stmt->sortClause,
										  &qry->targetList,
										  PGParseExprKind::EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );

	qry->groupClause = clause_parser.transformGroupClause(pstate,
											stmt->groupClause,
											&qry->groupingSets,
											&qry->targetList,
											qry->sortClause,
											PGParseExprKind::EXPR_KIND_GROUP_BY,
											false /* allow SQL92 rules */ );

	/*
	 * SCATTER BY clause on a table function TableValueExpr subquery.
	 *
	 * Note: a given subquery cannot have both a SCATTER clause and an INTO
	 * clause, because both of those control distribution.  This should not
	 * possible due to grammar restrictions on where a SCATTER clause is
	 * allowed.
	 */
	Assert(!(stmt->scatterClause && stmt->intoClause));
	// qry->scatterClause = transformScatterClause(pstate,
	// 											stmt->scatterClause,
	// 											&qry->targetList);

	if (stmt->distinctClause == NIL)
	{
		qry->distinctClause = NIL;
		qry->hasDistinctOn = false;
	}
	else if (linitial(stmt->distinctClause) == NULL)
	{
		/* We had SELECT DISTINCT */
		qry->distinctClause = clause_parser.transformDistinctClause(pstate,
													  &qry->targetList,
													  qry->sortClause,
													  false);
		qry->hasDistinctOn = false;
	}
	else
	{
		/* We had SELECT DISTINCT ON */
		qry->distinctClause = clause_parser.transformDistinctOnClause(pstate,
														stmt->distinctClause,
														&qry->targetList,
														qry->sortClause);
		qry->hasDistinctOn = true;
	}

	/* transform LIMIT */
	qry->limitOffset = clause_parser.transformLimitClause(pstate, stmt->limitOffset,
											PGParseExprKind::EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = clause_parser.transformLimitClause(pstate, stmt->limitCount,
										   PGParseExprKind::EXPR_KIND_LIMIT, "LIMIT");

	/* transform window clauses after we have seen all window functions */
	qry->windowClause = clause_parser.transformWindowDefinitions(pstate,
												   pstate->p_windowdefs,
												   &qry->targetList);

	/* resolve any still-unresolved output columns as being type text */
	if (pstate->p_resolve_unknowns)
		target_parser.resolveTargetListUnknowns(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;
	// qry->hasFuncsWithExecRestrictions = pstate->p_hasFuncsWithExecRestrictions;

	if (pstate->p_hasTblValueExpr)
		func_parser.parseCheckTableFunctions(pstate, qry);

	foreach(l, stmt->lockingClause)
	{
		transformLockingClause(pstate, qry,
							   (PGLockingClause *) lfirst(l), false);
	}

	collation_parser.assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
		agg_parser.parseCheckAggregates(pstate, qry);

	return qry;
};

PGQuery *
SelectParser::transformStmt(PGParseState *pstate, PGNode *parseTree)
{
    PGQuery	   *result;

	/*
	 * We apply RAW_EXPRESSION_COVERAGE_TEST testing to basic DML statements;
	 * we can't just run it on everything because raw_expression_tree_walker()
	 * doesn't claim to handle utility statements.
	 */
#ifdef RAW_EXPRESSION_COVERAGE_TEST
	switch (nodeTag(parseTree))
	{
		case T_PGSelectStmt:
		case T_PGInsertStmt:
		case T_PGUpdateStmt:
		case T_PGDeleteStmt:
			(void) test_raw_expression_coverage(parseTree, NULL);
			break;
		default:
			break;
	}
#endif							/* RAW_EXPRESSION_COVERAGE_TEST */

	switch (nodeTag(parseTree))
	{
			/*
			 * Optimizable statements
			 */
		// case T_PGInsertStmt:
		// 	result = transformInsertStmt(pstate, (PGInsertStmt *) parseTree);
		// 	break;

		// case T_PGDeleteStmt:
		// 	result = transformDeleteStmt(pstate, (PGDeleteStmt *) parseTree);
		// 	break;

		// case T_PGUpdateStmt:
		// 	result = transformUpdateStmt(pstate, (PGUpdateStmt *) parseTree);
		// 	break;

		case T_PGSelectStmt:
			{
				PGSelectStmt *n = (PGSelectStmt *) parseTree;

                if (n->op == PG_SETOP_NONE)
					result = transformSelectStmt(pstate, n);

				// if (n->valuesLists)
				// 	result = transformValuesClause(pstate, n);
				// else if (n->op == PG_SETOP_NONE)
				// 	result = transformSelectStmt(pstate, n);
				// else
				// 	result = transformSetOperationStmt(pstate, n);
			}
			break;

			/*
			 * Special cases
			 */
		// case T_PGDeclareCursorStmt:
		// 	result = transformDeclareCursorStmt(pstate,
		// 										(PGDeclareCursorStmt *) parseTree);
		// 	break;

		// case T_PGExplainStmt:
		// 	result = transformExplainStmt(pstate,
		// 								  (PGExplainStmt *) parseTree);
		// 	break;

		// case T_PGCreateTableAsStmt:
		// 	result = transformCreateTableAsStmt(pstate,
		// 										(PGCreateTableAsStmt *) parseTree);
		// 	break;

		// case T_PGCallStmt:
		// 	result = transformCallStmt(pstate,
		// 							   (PGCallStmt *) parseTree);
		// 	break;

		default:

			/*
			 * other statements don't require any transformation; just return
			 * the original parsetree with a Query node plastered on top.
			 */
			result = makeNode(PGQuery);
			result->commandType = PG_CMD_UTILITY;
			result->utilityStmt = (PGNode *) parseTree;
			break;
	}

	/* Mark as original query until we learn differently */
	result->querySource = PG_QSRC_ORIGINAL;
	result->canSetTag = true;

	// if (pstate->p_hasDynamicFunction)
	// 	result->hasDynamicFunctions = true;

	return result;
};

PGQuery *
SelectParser::parse_sub_analyze(PGNode *parseTree, PGParseState *parentParseState,
				  PGCommonTableExpr *parentCTE,
				  PGLockingClause *lockclause_from_parent,
				  bool resolve_unknowns)
{
	PGParseState *pstate = make_parsestate(parentParseState);
	PGQuery	   *query;

	pstate->p_parent_cte = parentCTE;
	pstate->p_lockclause_from_parent = lockclause_from_parent;
	//pstate->p_resolve_unknowns = resolve_unknowns;

	query = transformStmt(pstate, parseTree);

	free_parsestate(pstate);

	return query;
};

}