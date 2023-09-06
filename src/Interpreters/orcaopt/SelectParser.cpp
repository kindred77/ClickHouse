#include <Interpreters/orcaopt/SelectParser.h>

#include <Interpreters/orcaopt/ClauseParser.h>
#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/TargetParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/CTEParser.h>
//#include <Interpreters/orcaopt/CollationParser.h>
#include <Interpreters/orcaopt/AggParser.h>
#include <Interpreters/orcaopt/FuncParser.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

SelectParser::SelectParser(const ContextPtr& context_) : context(context_)
{
    clause_parser = std::make_shared<ClauseParser>(context);
    target_parser = std::make_shared<TargetParser>(context);
    coerce_parser = std::make_shared<CoerceParser>(context);
    node_parser = std::make_shared<NodeParser>(context);
    relation_parser = std::make_shared<RelationParser>(context);
    cte_parser = std::make_shared<CTEParser>(context);
    agg_parser = std::make_shared<AggParser>(context);
    func_parser = std::make_shared<FuncParser>(context);
    // relation_provider = std::make_shared<RelationProvider>(context);
};

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
	rte = relation_parser->GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
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
				PGTargetEntry *ste = relation_parser->get_tle_by_resno(rte->subquery->targetList,
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

				Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars))
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
				PGCommonTableExpr *cte = relation_parser->GetCTEForRTE(pstate, rte, netlevelsup);
				PGTargetEntry *ste;

				ste = relation_parser->get_tle_by_resno(GetCTETargetList(cte), attnum);
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

PGNode * SelectParser::map_sgr_mutator(PGNode * node, void * context_grouped_window)
{
    grouped_window_ctx * ctx = (grouped_window_ctx *)context_grouped_window;

    if (!node)
        return NULL;

    if (IsA(node, PGList))
    {
        PGListCell * lc;
        PGList * new_lst = NIL;

        foreach (lc, (PGList *)node)
        {
            PGNode * newnode = (PGNode *)lfirst(lc);
            newnode = map_sgr_mutator(newnode, ctx);
            new_lst = lappend(new_lst, newnode);
        }
        return (PGNode *)new_lst;
    }
    else if (IsA(node, PGSortGroupClause))
    {
        PGSortGroupClause * g = (PGSortGroupClause *)node;
        PGSortGroupClause * new_g = makeNode(PGSortGroupClause);
        memcpy(new_g, g, sizeof(PGSortGroupClause));
        new_g->tleSortGroupRef = ctx->sgr_map[g->tleSortGroupRef];
        return (PGNode *)new_g;
    }
	//TODO kindred
    // else if (IsA(node, GroupingClause))
    // {
    //     GroupingClause * gc = (GroupingClause *)node;
    //     GroupingClause * new_gc = makeNode(GroupingClause);
    //     memcpy(new_gc, gc, sizeof(GroupingClause));
    //     new_gc->groupsets = (List *)map_sgr_mutator((Node *)gc->groupsets, ctx);
    //     return (Node *)new_gc;
    // }

    return NULL; /* Never happens */
};

void SelectParser::init_grouped_window_context(grouped_window_ctx * ctx, PGQuery * qry)
{
    PGList * grp_tles;
    PGList * grp_sortops;
    PGList * grp_eqops;
    PGListCell * lc = NULL;
    PGIndex maxsgr = 0;

    pg_get_sortgroupclauses_tles(qry->groupClause, qry->targetList, &grp_tles, &grp_sortops, &grp_eqops);
    list_free(grp_sortops);
    maxsgr = maxSortGroupRef(grp_tles, true);

    ctx->subtlist = NIL;
    ctx->subgroupClause = NIL;

    /* Set up scratch space.
	 */

    ctx->subrtable = qry->rtable;

    /* Map input = outer query sortgroupref values to subquery values while building the
	 * subquery target list prefix. */
    ctx->sgr_map = (PGIndex *)palloc((maxsgr + 1) * sizeof(ctx->sgr_map[0]));
    foreach (lc, grp_tles)
    {
        PGTargetEntry * tle;
        PGIndex old_sgr;

        tle = (PGTargetEntry *)copyObject(lfirst(lc));
        old_sgr = tle->ressortgroupref;

        ctx->subtlist = lappend(ctx->subtlist, tle);
        tle->resno = list_length(ctx->subtlist);
        tle->ressortgroupref = tle->resno;
        tle->resjunk = false;

        ctx->sgr_map[old_sgr] = tle->ressortgroupref;
    }

    /* Miscellaneous scratch area. */
    ctx->call_depth = 0;
    ctx->tle = NULL;

    /* Revise grouping into ctx->subgroupClause */
    ctx->subgroupClause = (PGList *)map_sgr_mutator((PGNode *)qry->groupClause, ctx);
};

void SelectParser::discard_grouped_window_context(grouped_window_ctx * ctx)
{
    ctx->subtlist = NIL;
    ctx->subgroupClause = NIL;
    ctx->tle = NULL;
    if (ctx->sgr_map)
        pfree(ctx->sgr_map);
    ctx->sgr_map = NULL;
    ctx->subrtable = NULL;
};

PGQuery * SelectParser::transformGroupedWindows(PGParseState * pstate,
        PGQuery * qry)
{
    PGQuery * subq;
    PGRangeTblEntry * rte;
    PGRangeTblRef * ref;
    PGAlias * alias;
    bool hadSubLinks = qry->hasSubLinks;

    grouped_window_ctx ctx;

    Assert(qry->commandType == PG_CMD_SELECT)
    Assert(!PointerIsValid(qry->utilityStmt))
    Assert(qry->returningList == NIL)

    if (!qry->hasWindowFuncs || !(qry->groupClause || qry->hasAggs))
        return qry;

    /* Make the new subquery (Q'').  Note that (per SQL:2003) there
	 * can't be any window functions called in the WHERE, GROUP BY,
	 * or HAVING clauses.
	 */
    subq = makeNode(PGQuery);
    subq->commandType = PG_CMD_SELECT;
    subq->querySource = PG_QSRC_PARSER;
    subq->canSetTag = true;
    subq->utilityStmt = NULL;
    subq->resultRelation = 0;
    subq->hasAggs = qry->hasAggs;
    subq->hasWindowFuncs = false; /* reevaluate later */
    subq->hasSubLinks = qry->hasSubLinks; /* reevaluate later */

    /* Core of subquery input table expression: */
    subq->rtable = qry->rtable; /* before windowing */
    subq->jointree = qry->jointree; /* before windowing */
    subq->targetList = NIL; /* fill in later */

    subq->returningList = NIL;
    subq->groupClause = qry->groupClause; /* before windowing */
    subq->havingQual = qry->havingQual; /* before windowing */
    subq->windowClause = NIL; /* by construction */
    subq->distinctClause = NIL; /* after windowing */
    subq->sortClause = NIL; /* after windowing */
    subq->limitOffset = NULL; /* after windowing */
    subq->limitCount = NULL; /* after windowing */
    subq->rowMarks = NIL;
    subq->setOperations = NULL;

    /* Check if there is a window function in the join tree. If so
	 * we must mark hasWindowFuncs in the sub query as well.
	 */
    if (pg_contain_windowfuncs((PGNode *)subq->jointree))
        subq->hasWindowFuncs = true;

    /* Make the single range table entry for the outer query Q' as
	 * a wrapper for the subquery (Q'') currently under construction.
	 */
    rte = makeNode(PGRangeTblEntry);
    rte->rtekind = PG_RTE_SUBQUERY;
    rte->subquery = subq;
    rte->alias = NULL; /* fill in later */
    rte->eref = NULL; /* fill in later */
    rte->inFromCl = true;
	//TODO kindred
    //rte->requiredPerms = ACL_SELECT;
    /* Default?
	 * rte->inh = 0;
	 * rte->checkAsUser = 0;
	 * rte->pseudocols = 0;
	*/

    /* Make a reference to the new range table entry .
	 */
    ref = makeNode(PGRangeTblRef);
    ref->rtindex = 1;

    /* Set up context for mutating the target list.  Careful.
	 * This is trickier than it looks.  The context will be
	 * "primed" with grouping targets.
	 */
    init_grouped_window_context(&ctx, qry);

    /* Begin rewriting the outer query in place.
     */
    qry->hasAggs = false; /* by constuction */
    /* qry->hasSubLinks -- reevaluate later. */

    /* Core of outer query input table expression: */
    qry->rtable = list_make1(rte);
    qry->jointree = (PGFromExpr *)makeNode(PGFromExpr);
    qry->jointree->fromlist = list_make1(ref);
    qry->jointree->quals = NULL;
    /* qry->targetList -- to be mutated from Q to Q' below */

    qry->groupClause = NIL; /* by construction */
    qry->havingQual = NULL; /* by construction */

    /* Mutate the Q target list and windowClauses for use in Q' and, at the
	 * same time, update state with info needed to assemble the target list
	 * for the subquery (Q'').
	 */
    qry->targetList = (PGList *)grouped_window_mutator((PGNode *)qry->targetList, &ctx);
    qry->windowClause = (PGList *)grouped_window_mutator((PGNode *)qry->windowClause, &ctx);
    qry->hasSubLinks = checkExprHasSubLink((PGNode *)qry->targetList);

    /* New subquery fields
	 */
    subq->targetList = ctx.subtlist;
    subq->groupClause = ctx.subgroupClause;

    /* We always need an eref, but we shouldn't really need a filled in alias.
	 * However, view deparse (or at least the fix for MPP-2189) wants one.
	 */
    alias = make_replacement_alias(subq, "Window");
    rte->eref = (PGAlias *)copyObject(alias);
    rte->alias = alias;

    /* Accomodate depth change in new subquery, Q''.
	 */
    IncrementVarSublevelsUpInTransformGroupedWindows((PGNode *)subq, 1, 1);

    /* Might have changed. */
    subq->hasSubLinks = checkExprHasSubLink((PGNode *)subq);

    Assert(PointerIsValid(qry->targetList))
    Assert(IsA(qry->targetList, PGList))
    /* Use error instead of assertion to "use" hadSubLinks and keep compiler happy. */
    if (hadSubLinks != (qry->hasSubLinks || subq->hasSubLinks))
        elog(ERROR, "inconsistency detected in internal grouped windows transformation");

    discard_grouped_window_context(&ctx);

	//TODO kindred
    //assign_query_collations(pstate, subq);

    return qry;
};

PGQuery *
SelectParser::transformSelectStmt(PGParseState *pstate, PGSelectStmt *stmt)
{
    PGQuery * qry = makeNode(PGQuery);
    PGNode * qual;
    //PGListCell * l;

    qry->commandType = PG_CMD_SELECT;

    /* process the WITH clause independently of all else */
    if (stmt->withClause)
    {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = cte_parser->transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    /* Complain if we get called from someplace where INTO is not allowed */
    if (stmt->intoClause)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_SYNTAX_ERROR),
             errmsg("SELECT ... INTO is not allowed here"),
             parser_errposition(pstate, exprLocation((PGNode *)stmt->intoClause))));

    /* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
    pstate->p_locking_clause = stmt->lockingClause;

    /*
	 * Put WINDOW clause data into pstate so that window references know
	 * about them.
	 */
    pstate->p_windowdefs = stmt->windowClause;

    /* process the FROM clause */
    clause_parser->transformFromClause(pstate, stmt->fromClause);

    /* transform targetlist */
    qry->targetList = target_parser->transformTargetList(pstate, stmt->targetList, EXPR_KIND_SELECT_TARGET);

    /* mark column origins */
    markTargetListOrigins(pstate, qry->targetList);

    /* transform WHERE */
    qual = clause_parser->transformWhereClause(pstate, stmt->whereClause, EXPR_KIND_WHERE, "WHERE");

    /* initial processing of HAVING clause is much like WHERE clause */
    qry->havingQual = clause_parser->transformWhereClause(pstate, stmt->havingClause, EXPR_KIND_HAVING, "HAVING");

    /*
     * CDB: Untyped Const or Param nodes in a subquery in the FROM clause
     * might have been assigned proper types when we transformed the WHERE
     * clause, targetlist, etc.  Bring targetlist Var types up to date.
     */
    coerce_parser->fixup_unknown_vars_in_targetlist(pstate, qry->targetList);

    /*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
    qry->sortClause = clause_parser->transformSortClause(
        pstate, stmt->sortClause, &qry->targetList, EXPR_KIND_ORDER_BY, true /* fix unknowns */, false /* allow SQL92 rules */);

    qry->groupClause = clause_parser->transformGroupClause(
        pstate, stmt->groupClause, &qry->targetList, qry->sortClause, EXPR_KIND_GROUP_BY, false /* allow SQL92 rules */);

    /*
	 * SCATTER BY clause on a table function TableValueExpr subquery.
	 *
	 * Note: a given subquery cannot have both a SCATTER clause and an INTO
	 * clause, because both of those control distribution.  This should not
	 * possible due to grammar restrictions on where a SCATTER clause is
	 * allowed.
	 */
	//TODO kindred
    // Insist(!(stmt->scatterClause && stmt->intoClause));
    // qry->scatterClause = clause_parser->transformScatterClause(pstate, stmt->scatterClause, &qry->targetList);

    if (stmt->distinctClause == NIL)
    {
        qry->distinctClause = NIL;
        qry->hasDistinctOn = false;
    }
    else if (linitial(stmt->distinctClause) == NULL)
    {
        /* We had SELECT DISTINCT */
        if (!pstate->p_hasAggs && !pstate->p_hasWindowFuncs && qry->groupClause == NIL && qry->targetList != NIL)
        {
            /*
			 * MPP-15040
			 * turn distinct clause into grouping clause to make both sort-based
			 * and hash-based grouping implementations viable plan options
			 */
            qry->distinctClause = clause_parser->transformDistinctToGroupBy(pstate, &qry->targetList, &qry->sortClause, &qry->groupClause);
        }
        else
        {
            qry->distinctClause = clause_parser->transformDistinctClause(pstate, &qry->targetList, qry->sortClause, false);
        }
        qry->hasDistinctOn = false;
    }
    else
    {
        /* We had SELECT DISTINCT ON */
        qry->distinctClause = clause_parser->transformDistinctOnClause(pstate, stmt->distinctClause, &qry->targetList, qry->sortClause);
        qry->hasDistinctOn = true;
    }

    /* transform LIMIT */
    qry->limitOffset = clause_parser->transformLimitClause(pstate, stmt->limitOffset, EXPR_KIND_OFFSET, "OFFSET");
    qry->limitCount = clause_parser->transformLimitClause(pstate, stmt->limitCount, EXPR_KIND_LIMIT, "LIMIT");

    /* transform window clauses after we have seen all window functions */
    qry->windowClause = clause_parser->transformWindowDefinitions(pstate, pstate->p_windowdefs, &qry->targetList);

    clause_parser->processExtendedGrouping(pstate, qry->havingQual, qry->windowClause, qry->targetList);

    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	//TODO kindred
    //qry->hasFuncsWithExecRestrictions = pstate->p_hasFuncsWithExecRestrictions;
    qry->hasAggs = pstate->p_hasAggs;

    if (pstate->p_hasTblValueExpr)
        parseCheckTableFunctions(pstate, qry);

	//TODO kindred
    // foreach (l, stmt->lockingClause)
    // {
    //     transformLockingClause(pstate, qry, (PGLockingClause *)lfirst(l), false);
    // }

    //assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (pstate->p_hasAggs || qry->groupClause || qry->havingQual)
        agg_parser->parseCheckAggregates(pstate, qry);

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
    if (qry->hasWindowFuncs && (qry->groupClause || qry->hasAggs))
        transformGroupedWindows(pstate, qry);

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
				  PGLockingClause *lockclause_from_parent)
{
    PGParseState * pstate = make_parsestate(parentParseState);
    PGQuery * query;

    pstate->p_parent_cte = parentCTE;
    pstate->p_lockclause_from_parent = lockclause_from_parent;

    query = transformStmt(pstate, parseTree);

    free_parsestate(pstate);

    return query;
};

PGAlias * SelectParser::make_replacement_alias(PGQuery *qry, const char *aname)
{
    PGListCell * lc = NULL;
    char * name = NULL;
    PGAlias * alias = makeNode(PGAlias);
    PGAttrNumber attrno = 0;

    alias->aliasname = pstrdup(aname);
    alias->colnames = NIL;

    foreach (lc, qry->targetList)
    {
        PGTargetEntry * tle = (PGTargetEntry *)lfirst(lc);
        attrno++;

        if (tle->resname)
        {
            /* Prefer the target's resname. */
            name = pstrdup(tle->resname);
        }
        else if (IsA(tle->expr, PGVar))
        {
            /* If the target expression is a Var, use the name of the
			 * attribute in the query's range table. */
            PGVar * var = (PGVar *)tle->expr;
            PGRangeTblEntry * rte = rt_fetch(var->varno, qry->rtable);
            name = pstrdup(RelationProvider::get_rte_attribute_name(rte, var->varattno).c_str());
        }
        else
        {
            /* If all else, fails, generate a name based on position. */
            name = generate_positional_name(attrno);
        }

        alias->colnames = lappend(alias->colnames, makeString(name));
    }
    return alias;
};

}
