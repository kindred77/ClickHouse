#include <Interpreters/orcaopt/pgopt/AggParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

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
				(errcode(PG_ERRCODE_WINDOWING_ERROR),
				 errmsg("window function calls cannot be nested"),
				 parser_errposition(pstate,
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
		case PGParseExprKind::EXPR_KIND_NONE:
			Assert(false);		/* can't happen */
			break;
		case PGParseExprKind::EXPR_KIND_OTHER:
			/* Accept window func here; caller must throw error if wanted */
			break;
		case PGParseExprKind::EXPR_KIND_JOIN_ON:
		case PGParseExprKind::EXPR_KIND_JOIN_USING:
			err = _("window functions are not allowed in JOIN conditions");
			break;
		case PGParseExprKind::EXPR_KIND_FROM_SUBSELECT:
			/* can't get here, but just in case, throw an error */
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_FROM_FUNCTION:
			err = _("window functions are not allowed in functions in FROM");
			break;
		case PGParseExprKind::EXPR_KIND_WHERE:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_HAVING:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_FILTER:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_WINDOW_PARTITION:
		case PGParseExprKind::EXPR_KIND_WINDOW_ORDER:
		case PGParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE:
		case PGParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS:
			err = _("window functions are not allowed in window definitions");
			break;
		case PGParseExprKind::EXPR_KIND_SELECT_TARGET:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_INSERT_TARGET:
		case PGParseExprKind::EXPR_KIND_UPDATE_SOURCE:
		case PGParseExprKind::EXPR_KIND_UPDATE_TARGET:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_GROUP_BY:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_ORDER_BY:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_DISTINCT_ON:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_LIMIT:
		case PGParseExprKind::EXPR_KIND_OFFSET:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_RETURNING:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_VALUES:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_CHECK_CONSTRAINT:
		case PGParseExprKind::EXPR_KIND_DOMAIN_CHECK:
			err = _("window functions are not allowed in check constraints");
			break;
		case PGParseExprKind::EXPR_KIND_COLUMN_DEFAULT:
		case PGParseExprKind::EXPR_KIND_FUNCTION_DEFAULT:
			err = _("window functions are not allowed in DEFAULT expressions");
			break;
		case PGParseExprKind::EXPR_KIND_INDEX_EXPRESSION:
			err = _("window functions are not allowed in index expressions");
			break;
		case PGParseExprKind::EXPR_KIND_INDEX_PREDICATE:
			err = _("window functions are not allowed in index predicates");
			break;
		case PGParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM:
			err = _("window functions are not allowed in transform expressions");
			break;
		case PGParseExprKind::EXPR_KIND_EXECUTE_PARAMETER:
			err = _("window functions are not allowed in EXECUTE parameters");
			break;
		case PGParseExprKind::EXPR_KIND_TRIGGER_WHEN:
			err = _("window functions are not allowed in trigger WHEN conditions");
			break;
		case PGParseExprKind::EXPR_KIND_PARTITION_EXPRESSION:
			err = _("window functions are not allowed in partition key expression");
			break;

		case PGParseExprKind::EXPR_KIND_SCATTER_BY:
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
		ereport(ERROR,
				(errcode(PG_ERRCODE_WINDOWING_ERROR),
				 errmsg_internal("%s", err),
				 parser_errposition(pstate, wfunc->location)));
	if (errkind)
		ereport(ERROR,
				(errcode(PG_ERRCODE_WINDOWING_ERROR),
		/* translator: %s is name of a SQL construct, eg GROUP BY */
				 errmsg("window functions are not allowed in %s",
						ParseExprKindName(pstate->p_expr_kind)),
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
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("window \"%s\" does not exist", name),
					 parser_errposition(pstate, windef->location)));
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

void
AggParser::transformAggregateCall(PGParseState *pstate, PGAggref *agg,
					   PGList *args, PGList *aggorder, bool agg_distinct)
{
    PGList	   *tlist = NIL;
	PGList	   *torder = NIL;
	PGList	   *tdistinct = NIL;
	PGAttrNumber	attno = 1;
	int			save_next_resno;
	int			min_varlevel;
	ListCell   *lc;
	const char *err;
	bool		errkind;

	if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
	{
		/*
		 * For an ordered-set agg, the args list includes direct args and
		 * aggregated args; we must split them apart.
		 */
		int			numDirectArgs = list_length(args) - list_length(aggorder);
		PGList	   *aargs;
		ListCell   *lc2;

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
										 torder, tlist, sortby,
										 true /* fix unknowns */ );
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
									 PGParseExprKind::EXPR_KIND_ORDER_BY,
									 true /* fix unknowns */ ,
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

				if (sortcl->sortop == InvalidOid)
				{
					PGNode	   *expr = get_sortgroupclause_expr(sortcl, tlist);

					ereport(ERROR,
							(errcode(PG_ERRCODE_SYNTAX_ERROR),
							 errmsg("could not identify an ordering operator for type %s",
									format_type_be(exprType(expr))),
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
	min_varlevel = check_agg_arguments(pstate,
									   agg->aggdirectargs,
									   agg->args,
									   agg->aggfilter);
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
		case PGParseExprKind::EXPR_KIND_NONE:
			Assert(false);		/* can't happen */
			break;
		case PGParseExprKind::EXPR_KIND_OTHER:
			/* Accept aggregate here; caller must throw error if wanted */
			break;
		case PGParseExprKind::EXPR_KIND_JOIN_ON:
		case PGParseExprKind::EXPR_KIND_JOIN_USING:
			err = _("aggregate functions are not allowed in JOIN conditions");
			break;
		case PGParseExprKind::EXPR_KIND_FROM_SUBSELECT:
			/* Should only be possible in a LATERAL subquery */
			Assert(pstate->p_lateral_active);
			/* Aggregate scope rules make it worth being explicit here */
			err = _("aggregate functions are not allowed in FROM clause of their own query level");
			break;
		case PGParseExprKind::EXPR_KIND_FROM_FUNCTION:
			err = _("aggregate functions are not allowed in functions in FROM");
			break;
		case PGParseExprKind::EXPR_KIND_WHERE:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_HAVING:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_FILTER:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_WINDOW_PARTITION:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_WINDOW_ORDER:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE:
			err = _("aggregate functions are not allowed in window RANGE");
			break;
		case PGParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS:
			err = _("aggregate functions are not allowed in window ROWS");
			break;
		case PGParseExprKind::EXPR_KIND_SELECT_TARGET:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_INSERT_TARGET:
		case PGParseExprKind::EXPR_KIND_UPDATE_SOURCE:
		case PGParseExprKind::EXPR_KIND_UPDATE_TARGET:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_GROUP_BY:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_ORDER_BY:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_DISTINCT_ON:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_LIMIT:
		case PGParseExprKind::EXPR_KIND_OFFSET:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_RETURNING:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_VALUES:
			errkind = true;
			break;
		case PGParseExprKind::EXPR_KIND_CHECK_CONSTRAINT:
		case PGParseExprKind::EXPR_KIND_DOMAIN_CHECK:
			err = _("aggregate functions are not allowed in check constraints");
			break;
		case PGParseExprKind::EXPR_KIND_COLUMN_DEFAULT:
		case PGParseExprKind::EXPR_KIND_FUNCTION_DEFAULT:
			err = _("aggregate functions are not allowed in DEFAULT expressions");
			break;
		case PGParseExprKind::EXPR_KIND_INDEX_EXPRESSION:
			err = _("aggregate functions are not allowed in index expressions");
			break;
		case PGParseExprKind::EXPR_KIND_INDEX_PREDICATE:
			err = _("aggregate functions are not allowed in index predicates");
			break;
		case PGParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM:
			err = _("aggregate functions are not allowed in transform expressions");
			break;
		case PGParseExprKind::EXPR_KIND_EXECUTE_PARAMETER:
			err = _("aggregate functions are not allowed in EXECUTE parameters");
			break;
		case PGParseExprKind::EXPR_KIND_TRIGGER_WHEN:
			err = _("aggregate functions are not allowed in trigger WHEN conditions");
			break;
		case PGParseExprKind::EXPR_KIND_PARTITION_EXPRESSION:
			err = _("aggregate functions are not allowed in partition key expression");

		case PGParseExprKind::EXPR_KIND_SCATTER_BY:
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
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg_internal("%s", err),
				 parser_errposition(pstate, agg->location)));
	if (errkind)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
		/* translator: %s is name of a SQL construct, eg GROUP BY */
				 errmsg("aggregate functions are not allowed in %s",
						ParseExprKindName(pstate->p_expr_kind)),
				 parser_errposition(pstate, agg->location)));
};

int
AggParsercheck_agg_arguments(PGParseState *pstate,
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
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("aggregate function calls cannot be nested"),
				 parser_errposition(pstate, aggloc)));
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
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("outer-level aggregate cannot contain a lower-level variable in its direct arguments"),
					 parser_errposition(pstate,
									 locate_var_of_level((PGNode *) directargs,
													context.min_varlevel))));
		if (context.min_agglevel >= 0 && context.min_agglevel <= agglevel)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("aggregate function calls cannot be nested"),
					 parser_errposition(pstate,
									 locate_agg_of_level((PGNode *) directargs,
													context.min_agglevel))));
	}

	return agglevel;
};

}