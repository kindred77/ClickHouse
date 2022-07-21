#include <SelectParser.h>

namespace DB
{

std::shared_ptr<PGParseState>
SelectParser::make_parsestate(PGParseState *parentParseState)
{
	auto pstate = std::make_shared<PGParseState>();

	pstate->parentParseState = parentParseState;

	/* Fill in fields that don't start at null/false/zero */
	pstate->p_next_resno = 1;

	if (parentParseState)
	{
		pstate->p_sourcetext = parentParseState->p_sourcetext;
		/* all hooks are copied from parent */
		//pstate->p_pre_columnref_hook = parentParseState->p_pre_columnref_hook;
		//pstate->p_post_columnref_hook = parentParseState->p_post_columnref_hook;
		//pstate->p_paramref_hook = parentParseState->p_paramref_hook;
		//pstate->p_coerce_param_hook = parentParseState->p_coerce_param_hook;
		pstate->p_ref_hook_state = parentParseState->p_ref_hook_state;
	}

	return pstate;
};

duckdb_libpgquery::PGQuery *
SelectParser::transformStmt(PGParseState *pstate, duckdb_libpgquery::PGNode *parseTree)
{
	duckdb_libpgquery::PGQuery	   *result;

	switch (nodeTag(parseTree))
	{
			/*
			 * Optimizable statements
			 */
		/*case T_InsertStmt:
			result = transformInsertStmt(pstate, (InsertStmt *) parseTree);
			break;

		case T_DeleteStmt:
			result = transformDeleteStmt(pstate, (DeleteStmt *) parseTree);
			break;

		case T_UpdateStmt:
			result = transformUpdateStmt(pstate, (UpdateStmt *) parseTree);
			break;*/

		case duckdb_libpgquery::T_PGSelectStmt:
			{
				duckdb_libpgquery::PGSelectStmt *n = (duckdb_libpgquery::PGSelectStmt *) parseTree;

				if (n->op == duckdb_libpgquery::PG_SETOP_NONE)
					result = transformSelectStmt(pstate, n);
				/*if (n->valuesLists)
					result = transformValuesClause(pstate, n);
				else if (n->op == duckdb_libpgquery::PG_SETOP_NONE)
					result = transformSelectStmt(pstate, n);
				else
					result = transformSetOperationStmt(pstate, n);*/
			}
			break;

			/*
			 * Special cases
			 */
		/*case T_DeclareCursorStmt:
			result = transformDeclareCursorStmt(pstate,
											(DeclareCursorStmt *) parseTree);
			break;

		case T_ExplainStmt:
			result = transformExplainStmt(pstate,
										  (ExplainStmt *) parseTree);
			break;

		case T_CreateTableAsStmt:
			result = transformCreateTableAsStmt(pstate,
											(CreateTableAsStmt *) parseTree);
			break;*/

		default:

			/*
			 * other statements don't require any transformation; just return
			 * the original parsetree with a Query node plastered on top.
			 */
			result = makeNode(duckdb_libpgquery::PGQuery);
			result->commandType = duckdb_libpgquery::PG_CMD_UTILITY;
			result->utilityStmt = (duckdb_libpgquery::PGNode *) parseTree;
			break;
	}

	/* Mark as original query until we learn differently */
	result->querySource = duckdb_libpgquery::PG_QSRC_ORIGINAL;
	result->canSetTag = true;

	//if (pstate->p_hasDynamicFunction)
		//result->hasDynamicFunctions = true;

	return result;
};

duckdb_libpgquery::PGQuery *
SelectParser::parse_sub_analyze(duckdb_libpgquery::PGNode *parseTree, PGParseState *parentParseState,
				  duckdb_libpgquery::PGCommonTableExpr *parentCTE,
				  duckdb_libpgquery::PGLockingClause *lockclause_from_parent)
{
	PGParseState *pstate = make_parsestate(parentParseState).get();
	duckdb_libpgquery::PGQuery	   *query;

	pstate->p_parent_cte = parentCTE;
	pstate->p_lockclause_from_parent = lockclause_from_parent;

	query = transformStmt(pstate, parseTree);

	return query;
};

duckdb_libpgquery::PGQuery *
SelectParser::transformSelectStmt(PGParseState *pstate, duckdb_libpgquery::PGSelectStmt *stmt)
{
    duckdb_libpgquery::PGQuery	   *qry = makeNode(duckdb_libpgquery::PGQuery);
	duckdb_libpgquery::PGNode	   *qual;
	duckdb_libpgquery::PGListCell   *l;

	qry->commandType = duckdb_libpgquery::PGCmdType::PG_CMD_SELECT;

	/* process the WITH clause independently of all else */
	/*if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}*/

	/* Complain if we get called from someplace where INTO is not allowed */
	/*if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT ... INTO is not allowed here"),
				 parser_errposition(pstate,
								  exprLocation((duckdb_libpgquery::PGNodeNode *) stmt->intoClause))));*/

	/* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
	pstate->p_locking_clause = stmt->lockingClause;

	/*
	 * Put WINDOW clause data into pstate so that window references know
	 * about them.
	 */
	pstate->p_windowdefs = stmt->windowClause;

	/* process the FROM clause */
	clause_parser.transformFromClause(pstate, stmt->fromClause);

	/* transform targetlist */
	qry->targetList = target_parser.transformTargetList(pstate, stmt->targetList,
										  EXPR_KIND_SELECT_TARGET);

	/* mark column origins */
	target_parser.markTargetListOrigins(pstate, qry->targetList);

	/* transform WHERE */
	qual = clause_parser.transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");

	/* initial processing of HAVING clause is much like WHERE clause */
	qry->havingQual = clause_parser.transformWhereClause(pstate, stmt->havingClause,
										   EXPR_KIND_HAVING, "HAVING");

    /*
     * CDB: Untyped Const or Param nodes in a subquery in the FROM clause
     * might have been assigned proper types when we transformed the WHERE
     * clause, targetlist, etc.  Bring targetlist Var types up to date.
     */
    coerce_parser.fixup_unknown_vars_in_targetlist(pstate, qry->targetList);

	/*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
	qry->sortClause = clause_parser.transformSortClause(pstate,
										  stmt->sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  true /* fix unknowns */ ,
										  false /* allow SQL92 rules */ );

	qry->groupClause = clause_parser.transformGroupClause(pstate,
											stmt->groupClause,
											&qry->targetList,
											qry->sortClause,
											EXPR_KIND_GROUP_BY,
											false /* allow SQL92 rules */ );

	/*
	 * SCATTER BY clause on a table function TableValueExpr subquery.
	 *
	 * Note: a given subquery cannot have both a SCATTER clause and an INTO
	 * clause, because both of those control distribution.  This should not
	 * possible due to grammar restrictions on where a SCATTER clause is
	 * allowed.
	 */
	Insist(!(stmt->scatterClause && stmt->intoClause));
	// qry->scatterClause = clause_parser.transformScatterClause(pstate,
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
		if (!pstate->p_hasAggs && !pstate->p_hasWindowFuncs && qry->groupClause == NIL &&
			qry->targetList != NIL)
		{
			/*
			 * MPP-15040
			 * turn distinct clause into grouping clause to make both sort-based
			 * and hash-based grouping implementations viable plan options
			 */
			qry->distinctClause = clause_parser.transformDistinctToGroupBy(pstate,
															 &qry->targetList,
															 &qry->sortClause,
															 &qry->groupClause);
		}
		else
		{
			qry->distinctClause = clause_parser.transformDistinctClause(pstate,
														  &qry->targetList,
														  qry->sortClause,
														  false);
		}
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
											EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = clause_parser.transformLimitClause(pstate, stmt->limitCount,
										   EXPR_KIND_LIMIT, "LIMIT");

	/* transform window clauses after we have seen all window functions */
	qry->windowClause = transformWindowDefinitions(pstate,
												   pstate->p_windowdefs,
												   &qry->targetList);

	//processExtendedGrouping(pstate, qry->havingQual, qry->windowClause, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	//qry->hasFuncsWithExecRestrictions = pstate->p_hasFuncsWithExecRestrictions;
	qry->hasAggs = pstate->p_hasAggs;

	// if (pstate->p_hasTblValueExpr)
	// 	parseCheckTableFunctions(pstate, qry);

	// foreach(l, stmt->lockingClause)
	// {
	// 	transformLockingClause(pstate, qry,
	// 						   (LockingClause *) lfirst(l), false);
	// }

	// assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	// if (pstate->p_hasAggs || qry->groupClause || qry->havingQual)
	// 	parseCheckAggregates(pstate, qry);

	/*
	 * If the query mixes window functions and aggregates, we need to
	 * transform it such that the grouped query appears as a subquery
	 *
	 * This must be done after collations. Because it, specifically the
	 * grouped_window_mutator() it called, will replace some expressions with
	 * Var and set the varcollid with the replaced expressions' original
	 * collations, which are from assign_query_collations().
	 *
	 * Note: assign_query_collations() doesn't handle Var's collation.
	 */
	// if (qry->hasWindowFuncs && (qry->groupClause || qry->hasAggs))
	// 	transformGroupedWindows(pstate, qry);

	return qry;
};

}