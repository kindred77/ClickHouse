#include <Interpreters/orcaopt/AggParser.h>

#include <Interpreters/orcaopt/walkers.h>
#include <Interpreters/orcaopt/equalfuncs.h>

#include <Interpreters/orcaopt/ExprParser.h>

using namespace duckdb_libpgquery;

#ifdef __clang__
#pragma clang diagnostic ignored "-Wswitch"
#else
#pragma GCC diagnostic ignored "-Wswitch"
#endif

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

        sb->node = (PGNode *)expr_parser_ptr->transformExpr(pstate, sb->node);
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

        sb->node = (PGNode *)expr_parser_ptr->transformExpr(pstate, sb->node);
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

// void AggParser::check_call(PGParseState * pstate, PGNode * call)
// {
//     int min_varlevel = -1;
//     bool is_agg = IsA(call, PGAggref);

//     /*
// 	 * The call's level is the same as the level of the lowest-level
// 	 * variable or aggregate in its arguments; or if it contains no variables
// 	 * at all, we presume it to be local.
// 	 */
//     if (is_agg)
//         min_varlevel = find_minimum_var_level((PGNode *)((PGAggref *)call)->args);
//     else
//         min_varlevel = find_minimum_var_level((PGNode *)((PGWindowDef *)call)->args);

//     /*
// 	 * An aggregate can't directly contain another aggregate call of the same
// 	 * level (though outer aggs are okay).	We can skip this check if we
// 	 * didn't find any local vars or aggs.
// 	 */
//     if (min_varlevel == 0 && is_agg)
//     {
//         if (checkExprHasAggs((PGNode *)((PGAggref *)call)->args))
//             ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("aggregate function calls may not be nested")/* , errOmitLocation(true) */));

//         if (checkExprHasWindFuncs((PGNode *)((PGAggref *)call)->args))
//         {
//             ereport(
//                 ERROR,
//                 (errcode(ERRCODE_GROUPING_ERROR),
//                  errmsg("window functions may not be used as arguments to "
//                         "aggregates")/* ,
//                  errOmitLocation(true) */));
//         }
//     }

//     /*
// 	 * Window functions, on the other hand, may contain nested aggregates
// 	 * but not nested window refs.
// 	 */
//     if (min_varlevel == 0 && !is_agg)
//     {
//         if (checkExprHasWindFuncs((PGNode *)((PGWindowDef *)call)->args))
//         {
//             ereport(
//                 ERROR,
//                 (errcode(ERRCODE_GROUPING_ERROR),
//                  errmsg("cannot use window function as an argument "
//                         "to another window function")/* ,
//                  errOmitLocation(true) */));
//         }
//     }

//     if (min_varlevel < 0)
//         min_varlevel = 0;

//     if (is_agg)
//         ((PGAggref *)call)->agglevelsup = min_varlevel;
//     else
//         ((PGWindowDef *)call)->winlevelsup = min_varlevel;

//     /* Mark the correct pstate as having aggregates */
//     while (min_varlevel-- > 0)
//         pstate = pstate->parentParseState;

//     if (is_agg)
//         pstate->p_hasAggs = true;
//     else
//         pstate->p_hasWindowFuncs = true;
// };

void AggParser::transformWindowFuncCall(PGParseState * pstate, PGWindowFunc * wfunc, PGWindowDef * windef)
{
	//check_call(pstate, (PGNode *)wind);

    const char * err;
    bool errkind;
    char * name;

    /*
	 * A window function call can't contain another one (but aggs are OK). XXX
	 * is this required by spec, or just an unimplemented feature?
	 *
	 * Note: we don't need to check the filter expression here, because the
	 * context checks done below and in transformAggregateCall would have
	 * already rejected any window funcs or aggs within the filter.
	 */
    if (pstate->p_hasWindowFuncs && pg_contain_windowfuncs((PGNode *)wfunc->args))
        ereport(
            ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
             errmsg("window function calls cannot be nested"),
             parser_errposition(pstate, pg_locate_windowfunc((PGNode *)wfunc->args))));

    /*
	 * Check to see if the window function is in an invalid place within the
	 * query.
	 *
	 * For brevity we support two schemes for reporting an error here: set
	 * "err" to a custom message, or set "errkind" true if the error context
	 * is sufficiently identified by what ParseExprKindName will return, *and*
	 * what it will return is just a SQL keyword.  (Otherwise, use a custom
	 * message to avoid creating translation problems.)
	 */
    err = NULL;
    errkind = false;
    switch (pstate->p_expr_kind)
    {
        case EXPR_KIND_NONE:
            Assert(false) /* can't happen */
            break;
        case EXPR_KIND_OTHER:
            /* Accept window func here; caller must throw error if wanted */
            break;
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
            err = _("window functions are not allowed in JOIN conditions");
            break;
        case EXPR_KIND_FROM_SUBSELECT:
            /* can't get here, but just in case, throw an error */
            errkind = true;
            break;
        case EXPR_KIND_FROM_FUNCTION:
            err = _("window functions are not allowed in functions in FROM");
            break;
        case EXPR_KIND_WHERE:
            errkind = true;
            break;
        case EXPR_KIND_HAVING:
            errkind = true;
            break;
        case EXPR_KIND_FILTER:
            errkind = true;
            break;
        case EXPR_KIND_WINDOW_PARTITION:
        case EXPR_KIND_WINDOW_ORDER:
        case EXPR_KIND_WINDOW_FRAME_RANGE:
        case EXPR_KIND_WINDOW_FRAME_ROWS:
            err = _("window functions are not allowed in window definitions");
            break;
        case EXPR_KIND_SELECT_TARGET:
            /* okay */
            break;
        case EXPR_KIND_INSERT_TARGET:
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
            errkind = true;
            break;
        case EXPR_KIND_GROUP_BY:
            errkind = true;
            break;
        case EXPR_KIND_ORDER_BY:
            /* okay */
            break;
        case EXPR_KIND_DISTINCT_ON:
            /* okay */
            break;
        case EXPR_KIND_LIMIT:
        case EXPR_KIND_OFFSET:
            errkind = true;
            break;
        case EXPR_KIND_RETURNING:
            errkind = true;
            break;
        case EXPR_KIND_VALUES:
            errkind = true;
            break;
        case EXPR_KIND_CHECK_CONSTRAINT:
        case EXPR_KIND_DOMAIN_CHECK:
            err = _("window functions are not allowed in check constraints");
            break;
        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:
            err = _("window functions are not allowed in DEFAULT expressions");
            break;
        case EXPR_KIND_INDEX_EXPRESSION:
            err = _("window functions are not allowed in index expressions");
            break;
        case EXPR_KIND_INDEX_PREDICATE:
            err = _("window functions are not allowed in index predicates");
            break;
        case EXPR_KIND_ALTER_COL_TRANSFORM:
            err = _("window functions are not allowed in transform expressions");
            break;
        case EXPR_KIND_EXECUTE_PARAMETER:
            err = _("window functions are not allowed in EXECUTE parameters");
            break;
        case EXPR_KIND_TRIGGER_WHEN:
            err = _("window functions are not allowed in trigger WHEN conditions");
            break;
        case EXPR_KIND_PARTITION_EXPRESSION:
            err = _("window functions are not allowed in partition key expression");
            break;

        case EXPR_KIND_SCATTER_BY:
            /* okay */
            break;

            /*
			 * There is intentionally no default: case here, so that the
			 * compiler will warn if we add a new ParseExprKind without
			 * extending this switch.  If we do see an unrecognized value at
			 * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
			 * which is sane anyway.
			 */
    }
    if (err)
        ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg_internal("%s", err), parser_errposition(pstate, wfunc->location)));
    if (errkind)
        ereport(
            ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
             /* translator: %s is name of a SQL construct, eg GROUP BY */
             errmsg("window functions are not allowed in %s", expr_parser_ptr->ParseExprKindName(pstate->p_expr_kind)),
             parser_errposition(pstate, wfunc->location)));

    /*
	 * If the OVER clause just specifies a window name, find that WINDOW
	 * clause (which had better be present).  Otherwise, try to match all the
	 * properties of the OVER clause, and make a new entry in the p_windowdefs
	 * list if no luck.
	 *
	 * In PostgreSQL, the syntax for this is "agg() OVER w". In GPDB, we also
	 * accept "agg() OVER (w)", with the extra parens.
	 */
    if (windef->name)
    {
        name = windef->name;

        Assert(
            windef->refname == NULL && windef->partitionClause == NIL && windef->orderClause == NIL
            && windef->frameOptions == FRAMEOPTION_DEFAULTS)
    }
    else if (windef->refname && !windef->partitionClause && !windef->orderClause && (windef->frameOptions & FRAMEOPTION_NONDEFAULT) == 0)
    {
        /* This is "agg() OVER (w)" */
        name = windef->refname;
    }
    else
        name = NULL;

    if (name)
    {
        Index winref = 0;
        PGListCell * lc;

        foreach (lc, pstate->p_windowdefs)
        {
            PGWindowDef * refwin = (PGWindowDef *)lfirst(lc);

            winref++;
            if (refwin->name && strcmp(refwin->name, name) == 0)
            {
                wfunc->winref = winref;
                break;
            }
        }
        if (lc == NULL) /* didn't find it? */
            ereport(
                ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("window \"%s\" does not exist", name),
                 parser_errposition(pstate, windef->location)));
    }
    else
    {
        Index winref = 0;
        PGListCell * lc;

        foreach (lc, pstate->p_windowdefs)
        {
            PGWindowDef * refwin = (PGWindowDef *)lfirst(lc);

            winref++;
            if (refwin->refname && windef->refname && strcmp(refwin->refname, windef->refname) == 0)
                /* matched on refname */;
            else if (!refwin->refname && !windef->refname)
                /* matched, no refname */;
            else
                continue;
            if (pg_equal(refwin->partitionClause, windef->partitionClause) && pg_equal(refwin->orderClause, windef->orderClause)
                && refwin->frameOptions == windef->frameOptions && pg_equal(refwin->startOffset, windef->startOffset)
                && pg_equal(refwin->endOffset, windef->endOffset))
            {
                /* found a duplicate window specification */
                wfunc->winref = winref;
                break;
            }
        }
        if (lc == NULL) /* didn't find it? */
        {
            pstate->p_windowdefs = lappend(pstate->p_windowdefs, windef);
            wfunc->winref = list_length(pstate->p_windowdefs);
        }
    }

    pstate->p_hasWindowFuncs = true;
};

}
