#include <Interpreters/orcaopt/AggParser.h>

#include <Interpreters/orcaopt/ClauseParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/ExprParser.h>
#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

// AggParser::AggParser(const ContextPtr& context_) : context(context_)
// {
// 	clause_parser = std::make_shared<ClauseParser>(context);
// 	node_parser = std::make_shared<NodeParser>(context);
// 	expr_parser = std::make_shared<ExprParser>(context);
// };

int AggParser::check_agg_arguments(PGParseState * pstate, PGList * directargs, PGList * args, PGExpr * filter)
{
    int agglevel;
    check_agg_arguments_context check_agg_context;

    check_agg_context.pstate = pstate;
    check_agg_context.min_varlevel = -1; /* signifies nothing found yet */
    check_agg_context.min_agglevel = -1;
    check_agg_context.sublevels_up = 0;

    (void)pg_expression_tree_walker((PGNode *)args, (walker_func)check_agg_arguments_walker, (void *)&check_agg_context);

    (void)pg_expression_tree_walker((PGNode *)filter, (walker_func)check_agg_arguments_walker, (void *)&check_agg_context);

    /*
	 * If we found no vars nor aggs at all, it's a level-zero aggregate;
	 * otherwise, its level is the minimum of vars or aggs.
	 */
    if (check_agg_context.min_varlevel < 0)
    {
        if (check_agg_context.min_agglevel < 0)
            agglevel = 0;
        else
            agglevel = check_agg_context.min_agglevel;
    }
    else if (check_agg_context.min_agglevel < 0)
        agglevel = check_agg_context.min_varlevel;
    else
        agglevel = Min(check_agg_context.min_varlevel, check_agg_context.min_agglevel);

    /*
	 * If there's a nested aggregate of the same semantic level, complain.
	 */
    if (agglevel == check_agg_context.min_agglevel)
    {
        int aggloc;

        aggloc = pg_locate_agg_of_level((PGNode *)args, agglevel);
        if (aggloc < 0)
            aggloc = pg_locate_agg_of_level((PGNode *)filter, agglevel);
        parser_errposition(pstate, aggloc);
        ereport(ERROR, (errcode(PG_ERRCODE_GROUPING_ERROR), errmsg("aggregate function calls cannot be nested")));
    }

    /*
	 * Now check for vars/aggs in the direct arguments, and throw error if
	 * needed.  Note that we allow a Var of the agg's semantic level, but not
	 * an Agg of that level.  In principle such Aggs could probably be
	 * supported, but it would create an ordering dependency among the
	 * aggregates at execution time.  Since the case appears neither to be
	 * required by spec nor particularly useful, we just treat it as a
	 * nested-aggregate situation.
	 */
    if (directargs)
    {
        check_agg_context.min_varlevel = -1;
        check_agg_context.min_agglevel = -1;
        (void)pg_expression_tree_walker((PGNode *)directargs, (walker_func)check_agg_arguments_walker, (void *)&check_agg_context);
        if (check_agg_context.min_varlevel >= 0 && check_agg_context.min_varlevel < agglevel)
        {
            parser_errposition(pstate, pg_locate_var_of_level((PGNode *)directargs, check_agg_context.min_varlevel));
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_GROUPING_ERROR),
                 errmsg("outer-level aggregate cannot contain a lower-level variable in its direct arguments")));
        }
        if (check_agg_context.min_agglevel >= 0 && check_agg_context.min_agglevel <= agglevel)
        {
            parser_errposition(pstate, pg_locate_agg_of_level((PGNode *)directargs, check_agg_context.min_agglevel));
            ereport(ERROR, (errcode(PG_ERRCODE_GROUPING_ERROR), errmsg("aggregate function calls cannot be nested")));
        }
    }
    return agglevel;
};

void
AggParser::check_agglevels_and_constraints(PGParseState *pstate, PGNode *expr)
{
    PGList	   *directargs = NIL;
	PGList	   *args = NIL;
	PGExpr	   *filter = NULL;
	int			min_varlevel;
	int			location = -1;
	PGIndex	   *p_levelsup;
	const char *err;
	bool		errkind;
	bool		isAgg = IsA(expr, PGAggref);

	if (isAgg)
	{
		PGAggref	   *agg = (PGAggref *) expr;

		directargs = agg->aggdirectargs;
		args = agg->args;
		filter = agg->aggfilter;
		location = agg->location;
		p_levelsup = &agg->agglevelsup;
	}
	// else if (IsA(expr, PGGroupId))
	// {
	// 	PGGroupId *grp = (PGGroupId *) expr;

	// 	args = NIL;
	// 	location = grp->location;
	// 	p_levelsup = &grp->agglevelsup;
	// }
	else
	{
		PGGroupingFunc *grp = (PGGroupingFunc *) expr;

		args = grp->args;
		location = grp->location;
		p_levelsup = &grp->agglevelsup;
	}

	/*
	 * Check the arguments to compute the aggregate's level and detect
	 * improper nesting.
	 */
	min_varlevel = check_agg_arguments(pstate,
									   directargs,
									   args,
									   filter);

	*p_levelsup = min_varlevel;

	/* Mark the correct pstate level as having aggregates */
	while (min_varlevel-- > 0)
		pstate = pstate->parentParseState;
	pstate->p_hasAggs = true;

	/*
	 * Check to see if the aggregate function is in an invalid place within
	 * its aggregation query.
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
			Assert(false)		/* can't happen */
			break;
		case EXPR_KIND_OTHER:
			/*
			 * Accept aggregate/grouping here; caller must throw error if
			 * wanted
			 */
			break;
		case EXPR_KIND_JOIN_ON:
		case EXPR_KIND_JOIN_USING:
			if (isAgg)
				err = _("aggregate functions are not allowed in JOIN conditions");
			else
				err = _("grouping operations are not allowed in JOIN conditions");

			break;
		case EXPR_KIND_FROM_SUBSELECT:
			/* Should only be possible in a LATERAL subquery */
			Assert(pstate->p_lateral_active)

			/*
			 * Aggregate/grouping scope rules make it worth being explicit
			 * here
			 */
			if (isAgg)
				err = _("aggregate functions are not allowed in FROM clause of their own query level");
			else
				err = _("grouping operations are not allowed in FROM clause of their own query level");

			break;
		case EXPR_KIND_FROM_FUNCTION:
			if (isAgg)
				err = _("aggregate functions are not allowed in functions in FROM");
			else
				err = _("grouping operations are not allowed in functions in FROM");

			break;
		case EXPR_KIND_WHERE:
			errkind = true;
			break;
		// case EXPR_KIND_POLICY:
		// 	if (isAgg)
		// 		err = _("aggregate functions are not allowed in policy expressions");
		// 	else
		// 		err = _("grouping operations are not allowed in policy expressions");

		// 	break;
		case EXPR_KIND_HAVING:
			/* okay */
			break;
		case EXPR_KIND_FILTER:
			errkind = true;
			break;
		case EXPR_KIND_WINDOW_PARTITION:
			/* okay */
			break;
		case EXPR_KIND_WINDOW_ORDER:
			/* okay */
			break;
		case EXPR_KIND_WINDOW_FRAME_RANGE:
			if (isAgg)
				err = _("aggregate functions are not allowed in window RANGE");
			else
				err = _("grouping operations are not allowed in window RANGE");

			break;
		case EXPR_KIND_WINDOW_FRAME_ROWS:
			if (isAgg)
				err = _("aggregate functions are not allowed in window ROWS");
			else
				err = _("grouping operations are not allowed in window ROWS");

			break;
		// case EXPR_KIND_WINDOW_FRAME_GROUPS:
		// 	if (isAgg)
		// 		err = _("aggregate functions are not allowed in window GROUPS");
		// 	else
		// 		err = _("grouping operations are not allowed in window GROUPS");

		// 	break;
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
		// case EXPR_KIND_VALUES_SINGLE:
			errkind = true;
			break;
		case EXPR_KIND_CHECK_CONSTRAINT:
		case EXPR_KIND_DOMAIN_CHECK:
			if (isAgg)
				err = _("aggregate functions are not allowed in check constraints");
			else
				err = _("grouping operations are not allowed in check constraints");

			break;
		case EXPR_KIND_COLUMN_DEFAULT:
		case EXPR_KIND_FUNCTION_DEFAULT:

			if (isAgg)
				err = _("aggregate functions are not allowed in DEFAULT expressions");
			else
				err = _("grouping operations are not allowed in DEFAULT expressions");

			break;
		case EXPR_KIND_INDEX_EXPRESSION:
			if (isAgg)
				err = _("aggregate functions are not allowed in index expressions");
			else
				err = _("grouping operations are not allowed in index expressions");

			break;
		case EXPR_KIND_INDEX_PREDICATE:
			if (isAgg)
				err = _("aggregate functions are not allowed in index predicates");
			else
				err = _("grouping operations are not allowed in index predicates");

			break;
		case EXPR_KIND_ALTER_COL_TRANSFORM:
			if (isAgg)
				err = _("aggregate functions are not allowed in transform expressions");
			else
				err = _("grouping operations are not allowed in transform expressions");

			break;
		case EXPR_KIND_EXECUTE_PARAMETER:
			if (isAgg)
				err = _("aggregate functions are not allowed in EXECUTE parameters");
			else
				err = _("grouping operations are not allowed in EXECUTE parameters");

			break;
		case EXPR_KIND_TRIGGER_WHEN:
			if (isAgg)
				err = _("aggregate functions are not allowed in trigger WHEN conditions");
			else
				err = _("grouping operations are not allowed in trigger WHEN conditions");

			break;
		case EXPR_KIND_SCATTER_BY:
			/* okay */
			break;
		// case EXPR_KIND_PARTITION_BOUND:
		// 	if (isAgg)
		// 		err = _("aggregate functions are not allowed in partition bound");
		// 	else
		// 		err = _("grouping operations are not allowed in partition bound");

		// 	break;
		case EXPR_KIND_PARTITION_EXPRESSION:
			if (isAgg)
				err = _("aggregate functions are not allowed in partition key expressions");
			else
				err = _("grouping operations are not allowed in partition key expressions");

			break;
		// case EXPR_KIND_GENERATED_COLUMN:

		// 	if (isAgg)
		// 		err = _("aggregate functions are not allowed in column generation expressions");
		// 	else
		// 		err = _("grouping operations are not allowed in column generation expressions");

		// 	break;

		// case EXPR_KIND_CALL_ARGUMENT:
		// 	if (isAgg)
		// 		err = _("aggregate functions are not allowed in CALL arguments");
		// 	else
		// 		err = _("grouping operations are not allowed in CALL arguments");

		// 	break;

		// case EXPR_KIND_COPY_WHERE:
		// 	if (isAgg)
		// 		err = _("aggregate functions are not allowed in COPY FROM WHERE conditions");
		// 	else
		// 		err = _("grouping operations are not allowed in COPY FROM WHERE conditions");

		// 	break;

			/*
			 * There is intentionally no default: case here, so that the
			 * compiler will warn if we add a new ParseExprKind without
			 * extending this switch.  If we do see an unrecognized value at
			 * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
			 * which is sane anyway.
			 */
	}

	if (err)
	{
		parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_GROUPING_ERROR),
				 errmsg_internal("%s", err)));
	}

	if (errkind)
	{
		if (isAgg)
			/* translator: %s is name of a SQL construct, eg GROUP BY */
			err = _("aggregate functions are not allowed in %s");
		else
			/* translator: %s is name of a SQL construct, eg GROUP BY */
			err = _("grouping operations are not allowed in %s");
		
		parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_GROUPING_ERROR),
				 errmsg_internal(err,
								 ExprParser::ParseExprKindName(pstate->p_expr_kind))));
	}
};

PGNode *
AggParser::transformGroupingFunc(PGParseState *pstate, PGGroupingFunc *p)
{
	PGListCell   *lc;
	PGList	   *args = p->args;
	PGList	   *result_list = NIL;
	PGGroupingFunc *result = makeNode(PGGroupingFunc);

	if (list_length(args) > 31)
	{
		parser_errposition(pstate, p->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("GROUPING must have fewer than 32 arguments")));
	}

	foreach(lc, args)
	{
		PGNode	   *current_result;

		current_result = ExprParser::transformExpr(pstate, (PGNode *) lfirst(lc), pstate->p_expr_kind);

		/* acceptability of expressions is checked later */

		result_list = lappend(result_list, current_result);
	}

	result->args = result_list;
	result->location = p->location;

	check_agglevels_and_constraints(pstate, (PGNode *) result);

	return (PGNode *) result;
};

void AggParser::transformAggregateCall(PGParseState * pstate,
		PGAggref * agg, 
		PGList * args, 
		PGList * aggorder, bool agg_distinct)
{
    PGList * tlist = NIL;
    PGList * torder = NIL;
    PGList * tdistinct = NIL;
    PGAttrNumber attno = 1;
    int save_next_resno;
    int min_varlevel;
    PGListCell * lc;
    const char * err;
    bool errkind;

    if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
    {
        /*
		 * For an ordered-set agg, the args list includes direct args and
		 * aggregated args; we must split them apart.
		 */
        int numDirectArgs = list_length(args) - list_length(aggorder);
        PGList * aargs;
        PGListCell * lc2;

        Assert(numDirectArgs >= 0)

        aargs = list_copy_tail(args, numDirectArgs);
        agg->aggdirectargs = list_truncate(args, numDirectArgs);

        /*
		 * Build a tlist from the aggregated args, and make a sortlist entry
		 * for each one.  Note that the expressions in the SortBy nodes are
		 * ignored (they are the raw versions of the transformed args); we are
		 * just looking at the sort information in the SortBy nodes.
		 */
        forboth(lc, aargs, lc2, aggorder)
        {
            PGExpr * arg = (PGExpr *)lfirst(lc);
            PGSortBy * sortby = (PGSortBy *)lfirst(lc2);
            PGTargetEntry * tle;

            /* We don't bother to assign column names to the entries */
            tle = makeTargetEntry(arg, attno++, NULL, false);
            tlist = lappend(tlist, tle);

            torder = ClauseParser::addTargetToSortList(pstate, tle, torder, tlist, sortby, true /* fix unknowns */);
        }

        /* Never any DISTINCT in an ordered-set agg */
        Assert(!agg_distinct)
    }
    else
    {
        /* Regular aggregate, so it has no direct args */
        agg->aggdirectargs = NIL;

        /*
		 * Transform the plain list of Exprs into a targetlist.
		 */
        foreach (lc, args)
        {
            PGExpr * arg = (PGExpr *)lfirst(lc);
            PGTargetEntry * tle;

            /* We don't bother to assign column names to the entries */
            tle = makeTargetEntry(arg, attno++, NULL, false);
            tlist = lappend(tlist, tle);
        }

        /*
		 * If we have an ORDER BY, transform it.  This will add columns to the
		 * tlist if they appear in ORDER BY but weren't already in the arg
		 * list.  They will be marked resjunk = true so we can tell them apart
		 * from regular aggregate arguments later.
		 *
		 * We need to mess with p_next_resno since it will be used to number
		 * any new targetlist entries.
		 */
        save_next_resno = pstate->p_next_resno;
        pstate->p_next_resno = attno;

        torder = ClauseParser::transformSortClause(pstate, aggorder, &tlist, EXPR_KIND_ORDER_BY, true /* fix unknowns */, true /* force SQL99 rules */);

        /*
		 * If we have DISTINCT, transform that to produce a distinctList.
		 */
        if (agg_distinct)
        {
            tdistinct = ClauseParser::transformDistinctClause(pstate, &tlist, torder, true);

            /*
			 * Remove this check if executor support for hashed distinct for
			 * aggregates is ever added.
			 */
            foreach (lc, tdistinct)
            {
                PGSortGroupClause * sortcl = (PGSortGroupClause *)lfirst(lc);

                if (!OidIsValid(sortcl->sortop))
                {
                    PGNode * expr = get_sortgroupclause_expr(sortcl, tlist);

                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_UNDEFINED_FUNCTION),
                         errmsg("could not identify an ordering operator for type %s"/* , format_type_be(exprType(expr)) */),
                         errdetail("Aggregates with DISTINCT must be able to sort their inputs."),
                         parser_errposition(pstate, exprLocation(expr))));
                }
            }
        }

        pstate->p_next_resno = save_next_resno;
    }

    /* Update the Aggref with the transformation results */
    agg->args = tlist;
    agg->aggorder = torder;
    agg->aggdistinct = tdistinct;

    /*
	 * Check the arguments to compute the aggregate's level and detect
	 * improper nesting.
	 */
    min_varlevel = check_agg_arguments(pstate, agg->aggdirectargs, agg->args, agg->aggfilter);
    agg->agglevelsup = min_varlevel;

    /* Mark the correct pstate level as having aggregates */
    while (min_varlevel-- > 0)
        pstate = pstate->parentParseState;
    pstate->p_hasAggs = true;

    /*
	 * Check to see if the aggregate function is in an invalid place within
	 * its aggregation query.
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
            /* Accept aggregate here; caller must throw error if wanted */
            break;
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
            err = _("aggregate functions are not allowed in JOIN conditions");
            break;
        case EXPR_KIND_FROM_SUBSELECT:
            /* Should only be possible in a LATERAL subquery */
            Assert(pstate->p_lateral_active)
            /* Aggregate scope rules make it worth being explicit here */
            err = _("aggregate functions are not allowed in FROM clause of their own query level");
            break;
        case EXPR_KIND_FROM_FUNCTION:
            err = _("aggregate functions are not allowed in functions in FROM");
            break;
        case EXPR_KIND_WHERE:
            errkind = true;
            break;
        case EXPR_KIND_HAVING:
            /* okay */
            break;
        case EXPR_KIND_FILTER:
            errkind = true;
            break;
        case EXPR_KIND_WINDOW_PARTITION:
            /* okay */
            break;
        case EXPR_KIND_WINDOW_ORDER:
            /* okay */
            break;
        case EXPR_KIND_WINDOW_FRAME_RANGE:
            err = _("aggregate functions are not allowed in window RANGE");
            break;
        case EXPR_KIND_WINDOW_FRAME_ROWS:
            err = _("aggregate functions are not allowed in window ROWS");
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
            err = _("aggregate functions are not allowed in check constraints");
            break;
        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:
            err = _("aggregate functions are not allowed in DEFAULT expressions");
            break;
        case EXPR_KIND_INDEX_EXPRESSION:
            err = _("aggregate functions are not allowed in index expressions");
            break;
        case EXPR_KIND_INDEX_PREDICATE:
            err = _("aggregate functions are not allowed in index predicates");
            break;
        case EXPR_KIND_ALTER_COL_TRANSFORM:
            err = _("aggregate functions are not allowed in transform expressions");
            break;
        case EXPR_KIND_EXECUTE_PARAMETER:
            err = _("aggregate functions are not allowed in EXECUTE parameters");
            break;
        case EXPR_KIND_TRIGGER_WHEN:
            err = _("aggregate functions are not allowed in trigger WHEN conditions");
            break;
        case EXPR_KIND_PARTITION_EXPRESSION:
            err = _("aggregate functions are not allowed in partition key expression");

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
        ereport(ERROR, (errcode(PG_ERRCODE_GROUPING_ERROR), errmsg_internal("%s", err), parser_errposition(pstate, agg->location)));
    if (errkind)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_GROUPING_ERROR),
             /* translator: %s is name of a SQL construct, eg GROUP BY */
             errmsg("aggregate functions are not allowed in %s", ExprParser::ParseExprKindName(pstate->p_expr_kind)),
             parser_errposition(pstate, agg->location)));
};

void
AggParser::transformWindowFuncCall(PGParseState *pstate, PGWindowFunc *wfunc,
						PGWindowDef *windef)
{
	const char *err;
	bool		errkind;
	char	   *name;

	/*
	 * A window function call can't contain another one (but aggs are OK). XXX
	 * is this required by spec, or just an unimplemented feature?
	 *
	 * Note: we don't need to check the filter expression here, because the
	 * context checks done below and in transformAggregateCall would have
	 * already rejected any window funcs or aggs within the filter.
	 */
	if (pstate->p_hasWindowFuncs &&
		pg_contain_windowfuncs((PGNode *) wfunc->args))
	{
		parser_errposition(pstate,
									pg_locate_windowfunc((PGNode *) wfunc->args));
		ereport(ERROR,
				(errcode(PG_ERRCODE_WINDOWING_ERROR),
				 errmsg("window function calls cannot be nested")));
	}

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
			Assert(false)		/* can't happen */
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
		case EXPR_KIND_POLICY:
			err = _("window functions are not allowed in policy expressions");
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
		case EXPR_KIND_WINDOW_FRAME_GROUPS:
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
		case EXPR_KIND_VALUES_SINGLE:
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
		case EXPR_KIND_SCATTER_BY:
			/* okay */
			break;
		case EXPR_KIND_PARTITION_BOUND:
			err = _("window functions are not allowed in partition bound");
			break;
		case EXPR_KIND_PARTITION_EXPRESSION:
			err = _("window functions are not allowed in partition key expressions");
			break;
		case EXPR_KIND_CALL_ARGUMENT:
			err = _("window functions are not allowed in CALL arguments");
			break;
		case EXPR_KIND_COPY_WHERE:
			err = _("window functions are not allowed in COPY FROM WHERE conditions");
			break;
		case EXPR_KIND_GENERATED_COLUMN:
			err = _("window functions are not allowed in column generation expressions");
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
	{
		parser_errposition(pstate, wfunc->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_WINDOWING_ERROR),
				 errmsg_internal("%s", err)));
	}

	if (errkind)
	{
		parser_errposition(pstate, wfunc->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_WINDOWING_ERROR),
		/* translator: %s is name of a SQL construct, eg GROUP BY */
				 errmsg("window functions are not allowed in %s",
						ExprParser::ParseExprKindName(pstate->p_expr_kind))));
	}

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

		Assert(windef->refname == NULL &&
			   windef->partitionClause == NIL &&
			   windef->orderClause == NIL &&
			   windef->frameOptions == FRAMEOPTION_DEFAULTS)
	}
	else if (windef->refname &&
			 !windef->partitionClause &&
			 !windef->orderClause &&
			 (windef->frameOptions & FRAMEOPTION_NONDEFAULT) == 0)
	{
		/* This is "agg() OVER (w)" */
		name = windef->refname;
	}
	else
		name = NULL;

	if (name)
	{
		PGIndex		winref = 0;
		PGListCell   *lc;

		foreach(lc, pstate->p_windowdefs)
		{
			PGWindowDef  *refwin = (PGWindowDef *) lfirst(lc);

			winref++;
			if (refwin->name && strcmp(refwin->name, name) == 0)
			{
				wfunc->winref = winref;
				break;
			}
		}
		if (lc == NULL)			/* didn't find it? */
		{
			parser_errposition(pstate, windef->location);
			ereport(ERROR,
					(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
					 errmsg("window \"%s\" does not exist", name)));
		}
	}
	else
	{
		PGIndex		winref = 0;
		PGListCell   *lc;

		foreach(lc, pstate->p_windowdefs)
		{
			PGWindowDef  *refwin = (PGWindowDef *) lfirst(lc);

			winref++;
			if (refwin->refname && windef->refname &&
				strcmp(refwin->refname, windef->refname) == 0)
				 /* matched on refname */ ;
			else if (!refwin->refname && !windef->refname)
				 /* matched, no refname */ ;
			else
				continue;
			if (equal(refwin->partitionClause, windef->partitionClause) &&
				equal(refwin->orderClause, windef->orderClause) &&
				refwin->frameOptions == windef->frameOptions &&
				equal(refwin->startOffset, windef->startOffset) &&
				equal(refwin->endOffset, windef->endOffset))
			{
				/* found a duplicate window specification */
				wfunc->winref = winref;
				break;
			}
		}
		if (lc == NULL)			/* didn't find it? */
		{
			pstate->p_windowdefs = lappend(pstate->p_windowdefs, windef);
			wfunc->winref = list_length(pstate->p_windowdefs);
		}
	}

	pstate->p_hasWindowFuncs = true;
};

PGList *
AggParser::expand_groupingset_node(PGGroupingSet *gs)
{
	PGList	   *result = NIL;

	switch (gs->kind)
	{
		case GROUPING_SET_EMPTY:
			result = list_make1(NIL);
			break;

		case GROUPING_SET_SIMPLE:
			result = list_make1(gs->content);
			break;

		case GROUPING_SET_ROLLUP:
			{
				PGList	   *rollup_val = gs->content;
				PGListCell   *lc;
				int			curgroup_size = list_length(gs->content);

				while (curgroup_size > 0)
				{
					PGList	   *current_result = NIL;
					int			i = curgroup_size;

					foreach(lc, rollup_val)
					{
						PGGroupingSet *gs_current = (PGGroupingSet *) lfirst(lc);

						Assert(gs_current->kind == GROUPING_SET_SIMPLE)

						current_result
							= list_concat(current_result,
										  list_copy(gs_current->content));

						/* If we are done with making the current group, break */
						if (--i == 0)
							break;
					}

					result = lappend(result, current_result);
					--curgroup_size;
				}

				result = lappend(result, NIL);
			}
			break;

		case GROUPING_SET_CUBE:
			{
				PGList	   *cube_list = gs->content;
				int			number_bits = list_length(cube_list);
				uint32		num_sets;
				uint32		i;

				/* parser should cap this much lower */
				Assert(number_bits < 31)

				num_sets = (1U << number_bits);

				for (i = 0; i < num_sets; i++)
				{
					PGList	   *current_result = NIL;
					PGListCell   *lc;
					uint32		mask = 1U;

					foreach(lc, cube_list)
					{
						PGGroupingSet *gs_current = (PGGroupingSet *) lfirst(lc);

						Assert(gs_current->kind == GROUPING_SET_SIMPLE)

						if (mask & i)
						{
							current_result
								= list_concat(current_result,
											  list_copy(gs_current->content));
						}

						mask <<= 1;
					}

					result = lappend(result, current_result);
				}
			}
			break;

		case GROUPING_SET_SETS:
			{
				ListCell   *lc;

				foreach(lc, gs->content)
				{
					PGList	   *current_result = expand_groupingset_node((PGGroupingSet *)lfirst(lc));

					result = list_concat(result, current_result);
				}
			}
			break;
	}

	return result;
};

int
cmp_list_len_asc(const void *a, const void *b)
{
	int			la = list_length(*(PGList *const *) a);
	int			lb = list_length(*(PGList *const *) b);

	return (la > lb) ? 1 : (la == lb) ? 0 : -1;
};

PGList *
AggParser::expand_grouping_sets(PGList *groupingSets, int limit)
{
	PGList	   *expanded_groups = NIL;
	PGList	   *result = NIL;
	double		numsets = 1;
	PGListCell   *lc;

	if (groupingSets == NIL)
		return NIL;

	foreach(lc, groupingSets)
	{
		PGList	   *current_result = NIL;
		PGGroupingSet *gs = (PGGroupingSet *)lfirst(lc);

		current_result = expand_groupingset_node(gs);

		Assert(current_result != NIL)

		numsets *= list_length(current_result);

		if (limit >= 0 && numsets > limit)
			return NIL;

		expanded_groups = lappend(expanded_groups, current_result);
	}

	/*
	 * Do cartesian product between sublists of expanded_groups. While at it,
	 * remove any duplicate elements from individual grouping sets (we must
	 * NOT change the number of sets though)
	 */

	foreach(lc, (PGList *) linitial(expanded_groups))
	{
		result = lappend(result, list_union_int(NIL, (PGList *) lfirst(lc)));
	}

	for_each_cell(lc, lnext(list_head(expanded_groups)))
	{
		PGList	   *p = (PGList *)lfirst(lc);
		PGList	   *new_result = NIL;
		PGListCell   *lc2;

		foreach(lc2, result)
		{
			PGList	   *q = (PGList *)lfirst(lc2);
			PGListCell   *lc3;

			foreach(lc3, p)
			{
				new_result = lappend(new_result,
									 list_union_int(q, (PGList *) lfirst(lc3)));
			}
		}
		result = new_result;
	}

	if (list_length(result) > 1)
	{
		int			result_len = list_length(result);
		PGList	  **buf = (PGList **)palloc(sizeof(PGList *) * result_len);
		PGList	  **ptr = buf;

		foreach(lc, result)
		{
			*ptr++ = (PGList *)lfirst(lc);
		}

		qsort(buf, result_len, sizeof(PGList *), cmp_list_len_asc);

		result = NIL;
		ptr = buf;

		while (result_len-- > 0)
			result = lappend(result, *ptr++);

		pfree(buf);
	}

	return result;
};

PGList * AggParser::get_groupclause_exprs(PGNode * grpcl,
		PGList * targetList)
{
    PGList * result = NIL;

    if (!grpcl)
        return result;

    Assert(IsA(grpcl, PGSortGroupClause) /* || IsA(grpcl, GroupingClause) */ || IsA(grpcl, PGList))

	//TODO kindred
    if (IsA(grpcl, PGSortGroupClause))
    {
        PGNode * node = get_sortgroupclause_expr((PGSortGroupClause *)grpcl, targetList);
        result = lappend(result, node);
    }

    /* else if (IsA(grpcl, PGGroupingClause))
    {
        PGListCell * l;
        PGGroupingClause * gc = (PGGroupingClause *)grpcl;

        foreach (l, gc->groupsets)
        {
            result = list_concat(result, get_groupclause_exprs((PGNode *)lfirst(l), targetList));
        }
    } */

    else
    {
        PGList * exprs = (PGList *)grpcl;
        PGListCell * lc;

        foreach (lc, exprs)
        {
            result = list_concat(result, get_groupclause_exprs((PGNode *)lfirst(lc), targetList));
        }
    }

    return result;
};

void AggParser::check_ungrouped_columns(
        PGNode * node, PGParseState * pstate, PGQuery * qry,
		PGList * groupClauses, bool have_non_var_grouping, 
		PGList ** func_grouped_rels)
{
	check_ungrouped_columns_context check_ungrouped_context;

	check_ungrouped_context.pstate = pstate;
	check_ungrouped_context.qry = qry;
	check_ungrouped_context.groupClauses = groupClauses;
	check_ungrouped_context.have_non_var_grouping = have_non_var_grouping;
	check_ungrouped_context.func_grouped_rels = func_grouped_rels;
	check_ungrouped_context.sublevels_up = 0;
	check_ungrouped_context.in_agg_direct_args = false;
	pg_check_ungrouped_columns_walker(node, &check_ungrouped_context);
};

bool AggParser::checkExprHasGroupExtFuncs(PGNode * node)
{
    checkHasGroupExtFuncs_context check_context;
    check_context.sublevels_up = 0;

    /*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
    return pg_query_or_expression_tree_walker(node, (walker_func)pg_checkExprHasGroupExtFuncs_walker, (void *)&check_context, 0);
};

void
AggParser::parseCheckAggregates(PGParseState *pstate, PGQuery *qry)
{
    PGList * groupClauses = NIL;
    bool have_non_var_grouping;
    PGList * func_grouped_rels = NIL;
    PGListCell * l;
    bool hasJoinRTEs;
    bool hasSelfRefRTEs;
    PGPlannerInfo * root;
    PGNode * clause;

    /* This should only be called if we found aggregates or grouping */
    Assert(pstate->p_hasAggs || qry->groupClause || qry->havingQual)

    /*
	 * Scan the range table to see if there are JOIN or self-reference CTE
	 * entries.  We'll need this info below.
	 */
    hasJoinRTEs = hasSelfRefRTEs = false;
    foreach (l, pstate->p_rtable)
    {
        PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(l);

        if (rte->rtekind == PG_RTE_JOIN)
            hasJoinRTEs = true;
        else if (rte->rtekind == PG_RTE_CTE && rte->self_reference)
            hasSelfRefRTEs = true;
    }

    /*
	 * Build a list of the acceptable GROUP BY expressions for use by
	 * check_ungrouped_columns().
	 */
    foreach (l, qry->groupClause)
    {
        PGNode * grpcl = (PGNode *)lfirst(l);
        PGList * exprs;
        PGListCell * l2;

        if (grpcl == NULL)
            continue;

		//TODO kindred
        //Assert(IsA(grpcl, PGSortGroupClause) || IsA(grpcl, PGGroupingClause))
		Assert(IsA(grpcl, PGSortGroupClause))

        exprs = get_groupclause_exprs(grpcl, qry->targetList);

        foreach (l2, exprs)
        {
            PGNode * expr = (PGNode *)lfirst(l2);

            // FIXME: Should this go into check_agg_arguments now?
            if (checkExprHasGroupExtFuncs(expr))
                ereport(ERROR, (errcode(PG_ERRCODE_GROUPING_ERROR), errmsg("grouping() or group_id() not allowed in GROUP BY clause")));
            groupClauses = lcons(expr, groupClauses);
        }
    }

    /*
	 * If there are join alias vars involved, we have to flatten them to the
	 * underlying vars, so that aliased and unaliased vars will be correctly
	 * taken as equal.  We can skip the expense of doing this if no rangetable
	 * entries are RTE_JOIN kind. We use the planner's flatten_join_alias_vars
	 * routine to do the flattening; it wants a PlannerInfo root node, which
	 * fortunately can be mostly dummy.
	 */
    if (hasJoinRTEs)
    {
        root = makeNode(PGPlannerInfo);
        root->parse = qry;
		//TODO kindred
        //root->planner_cxt = PGCurrentMemoryContext;
        root->hasJoinRTEs = true;

        groupClauses = (PGList *)pg_flatten_join_alias_vars(root, (PGNode *)groupClauses);
    }
    else
        root = NULL; /* keep compiler quiet */

    /*
	 * Detect whether any of the grouping expressions aren't simple Vars; if
	 * they're all Vars then we don't have to work so hard in the recursive
	 * scans.  (Note we have to flatten aliases before this.)
	 */
    have_non_var_grouping = false;
    foreach (l, groupClauses)
    {
        if (!IsA((PGNode *)lfirst(l), PGVar))
        {
            have_non_var_grouping = true;
            break;
        }
    }

    /*
	 * Check the targetlist and HAVING clause for ungrouped variables.
	 *
	 * Note: because we check resjunk tlist elements as well as regular ones,
	 * this will also find ungrouped variables that came from ORDER BY and
	 * WINDOW clauses.  For that matter, it's also going to examine the
	 * grouping expressions themselves --- but they'll all pass the test ...
	 */
    clause = (PGNode *)qry->targetList;
    if (hasJoinRTEs)
        clause = pg_flatten_join_alias_vars(root, clause);
    check_ungrouped_columns(clause, pstate, qry, groupClauses, have_non_var_grouping, &func_grouped_rels);

    clause = (PGNode *)qry->havingQual;
    if (hasJoinRTEs)
        clause = pg_flatten_join_alias_vars(root, clause);
    check_ungrouped_columns(clause, pstate, qry, groupClauses, have_non_var_grouping, &func_grouped_rels);

    /*
	 * Per spec, aggregates can't appear in a recursive term.
	 */
    if (pstate->p_hasAggs && hasSelfRefRTEs)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_INVALID_RECURSION),
             errmsg("aggregate functions are not allowed in a recursive query's recursive term"),
             parser_errposition(pstate, pg_locate_agg_of_level((PGNode *)qry, 0))));
};

}
