#include <Interpreters/orcaopt/pgopt_hawq/AggParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/walkers.h>

namespace DB
{
using namespace duckdb_libpgquery;

bool
check_agg_arguments_walker(PGNode *node,
						   check_agg_arguments_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGVar))
	{
		int			varlevelsup = ((PGVar *) node)->varlevelsup;

		/* convert levelsup to frame of reference of original query */
		varlevelsup -= context->sublevels_up;
		/* ignore local vars of subqueries */
		if (varlevelsup >= 0)
		{
			if (context->min_varlevel < 0 ||
				context->min_varlevel > varlevelsup)
				context->min_varlevel = varlevelsup;
		}
		return false;
	}
	if (IsA(node, PGAggref))
	{
		int			agglevelsup = ((PGAggref *) node)->agglevelsup;

		/* convert levelsup to frame of reference of original query */
		agglevelsup -= context->sublevels_up;
		/* ignore local aggs of subqueries */
		if (agglevelsup >= 0)
		{
			if (context->min_agglevel < 0 ||
				context->min_agglevel > agglevelsup)
				context->min_agglevel = agglevelsup;
		}
		/* no need to examine args of the inner aggregate */
		return false;
	}
	if (IsA(node, PGGroupingFunc))
	{
		int			agglevelsup = ((PGGroupingFunc *) node)->agglevelsup;

		/* convert levelsup to frame of reference of original query */
		agglevelsup -= context->sublevels_up;
		/* ignore local aggs of subqueries */
		if (agglevelsup >= 0)
		{
			if (context->min_agglevel < 0 ||
				context->min_agglevel > agglevelsup)
				context->min_agglevel = agglevelsup;
		}
		/* Continue and descend into subtree */
	}
	// if (IsA(node, PGGroupId))
	// {
	// 	int			agglevelsup = ((PGGroupId *) node)->agglevelsup;

	// 	/* convert levelsup to frame of reference of original query */
	// 	agglevelsup -= context->sublevels_up;
	// 	/* ignore local aggs of subqueries */
	// 	if (agglevelsup >= 0)
	// 	{
	// 		if (context->min_agglevel < 0 ||
	// 			context->min_agglevel > agglevelsup)
	// 			context->min_agglevel = agglevelsup;
	// 	}
	// 	/* Continue and descend into subtree */
	// }
	/*
	 * SRFs and window functions can be rejected immediately, unless we are
	 * within a sub-select within the aggregate's arguments; in that case
	 * they're OK.
	 */
	if (context->sublevels_up == 0)
	{
		if ((IsA(node, PGFuncExpr) &&((PGFuncExpr *) node)->funcretset) ||
			(IsA(node, PGOpExpr) &&((PGOpExpr *) node)->opretset))
		{
			parser_errposition(context->pstate, exprLocation(node));
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregate function calls cannot contain set-returning function calls"),
					 errhint("You might be able to move the set-returning function into a LATERAL FROM item.")));
		}
		if (IsA(node, PGWindowFunc))
		{
			parser_errposition(context->pstate,
										((PGWindowFunc *) node)->location);
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("aggregate function calls cannot contain window function calls")));
		}
	}
	if (IsA(node, PGQuery))
	{
		/* Recurse into subselects */
		bool		result;

		context->sublevels_up++;
		result = pg_query_tree_walker((PGQuery *) node,
								   (walker_func)check_agg_arguments_walker,
								   (void *) context,
								   0);
		context->sublevels_up--;
		return result;
	}

	return pg_expression_tree_walker(node,
								  (walker_func)check_agg_arguments_walker,
								  (void *) context);
};

void AggParser::transformWindowSpec(PGParseState * pstate, WindowSpec * spec)
{
    PGListCell * lc2;
    PGList * new = NIL;

    foreach (lc2, spec->partition)
    {
        PGNode * n = (PGNode *)lfirst(lc2);
        PGSortBy * sb;

        Assert(IsA(n, PGSortBy));

        sb = (PGSortBy *)n;

        sb->node = (PGNode *)transformExpr(pstate, sb->node);
        new = lappend(new, (void *)sb);
    }
    spec->partition = new;

    new = NIL;
    foreach (lc2, spec->order)
    {
        PGNode * n = (PGNode *)lfirst(lc2);
        PGSortBy * sb;

        Assert(IsA(n, PGSortBy));

        sb = (PGSortBy *)n;

        sb->node = (PGNode *)transformExpr(pstate, sb->node);
        new = lappend(new, (void *)sb);
    }
    spec->order = new;

    if (spec->frame)
    {
        PGWindowFrame * frame = spec->frame;

        if (frame->trail)
            frame->trail->val = transformExpr(pstate, frame->trail->val);
        if (frame->lead)
            frame->lead->val = transformExpr(pstate, frame->lead->val);
    }
};

void AggParser::transformWindowSpecExprs(PGParseState * pstate)
{
    PGListCell * lc;

    foreach (lc, pstate->p_win_clauses)
    {
        PGWindowSpec * s = (PGWindowSpec *)lfirst(lc);
        transformWindowSpec(pstate, s);
    }
};

}