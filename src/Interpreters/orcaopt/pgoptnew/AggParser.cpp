#include <Interpreters/orcaopt/pgoptnew/AggParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

int
AggParser::check_agg_arguments(PGParseState *pstate,
					PGList *directargs,
					PGList *args,
					PGExpr *filter)
{
    int			agglevel;
	check_agg_arguments_context context;

	context.pstate = pstate;
	context.min_varlevel = -1;	/* signifies nothing found yet */
	context.min_agglevel = -1;
	context.sublevels_up = 0;

	(void) expression_tree_walker((PGNode *) args,
								  check_agg_arguments_walker,
								  (void *) &context);

	(void) expression_tree_walker((PGNode *) filter,
								  check_agg_arguments_walker,
								  (void *) &context);

	/*
	 * If we found no vars nor aggs at all, it's a level-zero aggregate;
	 * otherwise, its level is the minimum of vars or aggs.
	 */
	if (context.min_varlevel < 0)
	{
		if (context.min_agglevel < 0)
			agglevel = 0;
		else
			agglevel = context.min_agglevel;
	}
	else if (context.min_agglevel < 0)
		agglevel = context.min_varlevel;
	else
		agglevel = Min(context.min_varlevel, context.min_agglevel);

	/*
	 * If there's a nested aggregate of the same semantic level, complain.
	 */
	if (agglevel == context.min_agglevel)
	{
		int			aggloc;

		aggloc = locate_agg_of_level((PGNode *) args, agglevel);
		if (aggloc < 0)
			aggloc = locate_agg_of_level((PGNode *) filter, agglevel);
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("aggregate function calls cannot be nested"),
				 node_parser.parser_errposition(pstate, aggloc)));
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
		context.min_varlevel = -1;
		context.min_agglevel = -1;
		(void) expression_tree_walker((PGNode *) directargs,
									  check_agg_arguments_walker,
									  (void *) &context);
		if (context.min_varlevel >= 0 && context.min_varlevel < agglevel)
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("outer-level aggregate cannot contain a lower-level variable in its direct arguments"),
					 node_parser.parser_errposition(pstate,
										locate_var_of_level((PGNode *) directargs,
															context.min_varlevel))));
		if (context.min_agglevel >= 0 && context.min_agglevel <= agglevel)
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("aggregate function calls cannot be nested"),
					 node_parser.parser_errposition(pstate,
										locate_agg_of_level((PGNode *) directargs,
															context.min_agglevel))));
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
	Index	   *p_levelsup;
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
			Assert(false);		/* can't happen */
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
			Assert(pstate->p_lateral_active);

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
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg_internal("%s", err),
				 node_parser.parser_errposition(pstate, location)));

	if (errkind)
	{
		if (isAgg)
			/* translator: %s is name of a SQL construct, eg GROUP BY */
			err = _("aggregate functions are not allowed in %s");
		else
			/* translator: %s is name of a SQL construct, eg GROUP BY */
			err = _("grouping operations are not allowed in %s");

		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg_internal(err,
								 expr_parser.ParseExprKindName(pstate->p_expr_kind)),
				 node_parser.parser_errposition(pstate, location)));
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
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("GROUPING must have fewer than 32 arguments"),
				 node_parser.parser_errposition(pstate, p->location)));

	foreach(lc, args)
	{
		PGNode	   *current_result;

		current_result = expr_parser.transformExpr(pstate, (PGNode *) lfirst(lc), pstate->p_expr_kind);

		/* acceptability of expressions is checked later */

		result_list = lappend(result_list, current_result);
	}

	result->args = result_list;
	result->location = p->location;

	check_agglevels_and_constraints(pstate, (PGNode *) result);

	return (PGNode *) result;
};

void
AggParser::transformAggregateCall(PGParseState *pstate, PGAggref *agg,
					   PGList *args,
					   PGList *aggorder, bool agg_distinct)
{
	PGList	   *argtypes = NIL;
	PGList	   *tlist = NIL;
	PGList	   *torder = NIL;
	PGList	   *tdistinct = NIL;
	PGAttrNumber	attno = 1;
	int			save_next_resno;
	PGListCell   *lc;

	/*
	 * Before separating the args into direct and aggregated args, make a list
	 * of their data type OIDs for use later.
	 */
	foreach(lc, args)
	{
		PGExpr	   *arg = (PGExpr *) lfirst(lc);

		argtypes = lappend_oid(argtypes, exprType((PGNode *) arg));
	}
	agg->aggargtypes = argtypes;

	if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
	{
		/*
		 * For an ordered-set agg, the args list includes direct args and
		 * aggregated args; we must split them apart.
		 */
		int			numDirectArgs = list_length(args) - list_length(aggorder);
		PGList	   *aargs;
		PGListCell   *lc2;

		Assert(numDirectArgs >= 0);

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
			PGExpr	   *arg = (PGExpr *) lfirst(lc);
			PGSortBy	   *sortby = (PGSortBy *) lfirst(lc2);
			PGTargetEntry *tle;

			/* We don't bother to assign column names to the entries */
			tle = makeTargetEntry(arg, attno++, NULL, false);
			tlist = lappend(tlist, tle);

			torder = clause_parser.addTargetToSortList(pstate, tle,
										 torder, tlist, sortby);
		}

		/* Never any DISTINCT in an ordered-set agg */
		Assert(!agg_distinct);
	}
	else
	{
		/* Regular aggregate, so it has no direct args */
		agg->aggdirectargs = NIL;

		/*
		 * Transform the plain list of Exprs into a targetlist.
		 */
		foreach(lc, args)
		{
			PGExpr	   *arg = (PGExpr *) lfirst(lc);
			PGTargetEntry *tle;

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

		torder = clause_parser.transformSortClause(pstate,
									 aggorder,
									 &tlist,
									 EXPR_KIND_ORDER_BY,
									 true /* force SQL99 rules */ );

		/*
		 * If we have DISTINCT, transform that to produce a distinctList.
		 */
		if (agg_distinct)
		{
			tdistinct = clause_parser.transformDistinctClause(pstate, &tlist, torder, true);

			/*
			 * Remove this check if executor support for hashed distinct for
			 * aggregates is ever added.
			 */
			foreach(lc, tdistinct)
			{
				PGSortGroupClause *sortcl = (PGSortGroupClause *) lfirst(lc);

				if (!OidIsValid(sortcl->sortop))
				{
					PGNode	   *expr = get_sortgroupclause_expr(sortcl, tlist);

					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("could not identify an ordering operator for type %s",
									format_type_be(exprType(expr))),
							 errdetail("Aggregates with DISTINCT must be able to sort their inputs."),
							 node_parser.parser_errposition(pstate, exprLocation(expr))));
				}
			}
		}

		pstate->p_next_resno = save_next_resno;
	}

	/* Update the Aggref with the transformation results */
	agg->args = tlist;
	agg->aggorder = torder;
	agg->aggdistinct = tdistinct;

	check_agglevels_and_constraints(pstate, (PGNode *) agg);
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
		contain_windowfuncs((PGNode *) wfunc->args))
		ereport(ERROR,
				(errcode(ERRCODE_WINDOWING_ERROR),
				 errmsg("window function calls cannot be nested"),
				 node_parser.parser_errposition(pstate,
									locate_windowfunc((PGNode *) wfunc->args))));

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
			Assert(false);		/* can't happen */
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
		ereport(ERROR,
				(errcode(ERRCODE_WINDOWING_ERROR),
				 errmsg_internal("%s", err),
				 node_parser.parser_errposition(pstate, wfunc->location)));
	if (errkind)
		ereport(ERROR,
				(errcode(ERRCODE_WINDOWING_ERROR),
		/* translator: %s is name of a SQL construct, eg GROUP BY */
				 errmsg("window functions are not allowed in %s",
						ParseExprKindName(pstate->p_expr_kind)),
				 node_parser.parser_errposition(pstate, wfunc->location)));

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
			   windef->frameOptions == FRAMEOPTION_DEFAULTS);
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
		Index		winref = 0;
		ListCell   *lc;

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
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("window \"%s\" does not exist", name),
					 node_parser.parser_errposition(pstate, windef->location)));
	}
	else
	{
		Index		winref = 0;
		ListCell   *lc;

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

}