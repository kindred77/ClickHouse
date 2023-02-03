#include <Interpreters/orcaopt/pgopt_hawq/AggParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/walkers.h>

using namespace duckdb_libpgquery;

namespace DB
{

void AggParser::transformWindowSpec(PGParseState * pstate, PGWindowDef * spec)
{
    PGListCell * lc2;
    PGList * nl = NIL;

    foreach (lc2, spec->partitionClause)
    {
        PGNode * n = (PGNode *)lfirst(lc2);
        PGSortBy * sb;

        Assert(IsA(n, PGSortBy))

        sb = (PGSortBy *)n;

        sb->node = (PGNode *)expr_parser.transformExpr(pstate, sb->node);
        nl = lappend(nl, (void *)sb);
    }
    spec->partitionClause = nl;

    nl = NIL;
    foreach (lc2, spec->orderClause)
    {
        PGNode * n = (PGNode *)lfirst(lc2);
        PGSortBy * sb;

        Assert(IsA(n, PGSortBy))

        sb = (PGSortBy *)n;

        sb->node = (PGNode *)expr_parser.transformExpr(pstate, sb->node);
        nl = lappend(nl, (void *)sb);
    }
    spec->orderClause = nl;

	//TODO
    // if (spec->frame)
    // {
    //     PGWindowFrame * frame = spec->frame;

    //     if (frame->trail)
    //         frame->trail->val = transformExpr(pstate, frame->trail->val);
    //     if (frame->lead)
    //         frame->lead->val = transformExpr(pstate, frame->lead->val);
    // }
};

void AggParser::transformWindowSpecExprs(PGParseState * pstate)
{
    PGListCell * lc;

    foreach (lc, pstate->p_windowdefs)
    {
        PGWindowDef * s = (PGWindowDef *)lfirst(lc);
        transformWindowSpec(pstate, s);
    }
};

void AggParser::check_call(PGParseState * pstate, PGNode * call)
{
    int min_varlevel = -1;
    bool is_agg = IsA(call, PGAggref);

    /*
	 * The call's level is the same as the level of the lowest-level
	 * variable or aggregate in its arguments; or if it contains no variables
	 * at all, we presume it to be local.
	 */
    if (is_agg)
        min_varlevel = find_minimum_var_level((PGNode *)((PGAggref *)call)->args);
    else
        min_varlevel = find_minimum_var_level((PGNode *)((PGWindowDef *)call)->args);

    /*
	 * An aggregate can't directly contain another aggregate call of the same
	 * level (though outer aggs are okay).	We can skip this check if we
	 * didn't find any local vars or aggs.
	 */
    if (min_varlevel == 0 && is_agg)
    {
        if (checkExprHasAggs((PGNode *)((PGAggref *)call)->args))
            ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("aggregate function calls may not be nested"), errOmitLocation(true)));

        if (checkExprHasWindFuncs((PGNode *)((PGAggref *)call)->args))
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                 errmsg("window functions may not be used as arguments to "
                        "aggregates"),
                 errOmitLocation(true)));
        }
    }

    /*
	 * Window functions, on the other hand, may contain nested aggregates
	 * but not nested window refs.
	 */
    if (min_varlevel == 0 && !is_agg)
    {
        if (checkExprHasWindFuncs((PGNode *)((PGWindowDef *)call)->args))
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                 errmsg("cannot use window function as an argument "
                        "to another window function"),
                 errOmitLocation(true)));
        }
    }

    if (min_varlevel < 0)
        min_varlevel = 0;

    if (is_agg)
        ((PGAggref *)call)->agglevelsup = min_varlevel;
    else
        ((PGWindowDef *)call)->winlevelsup = min_varlevel;

    /* Mark the correct pstate as having aggregates */
    while (min_varlevel-- > 0)
        pstate = pstate->parentParseState;

    if (is_agg)
        pstate->p_hasAggs = true;
    else
        pstate->p_hasWindowFuncs = true;
};

void AggParser::transformWindowFuncCall(PGParseState * pstate, PGWindowDef * wind)
{
	check_call(pstate, (PGNode *)wind);
};

}
