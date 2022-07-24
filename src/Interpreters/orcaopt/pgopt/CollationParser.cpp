#include <CollationParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

void
CollationParser::assign_expr_collations(PGParseState *pstate, PGNode *expr)
{
    assign_collations_context context;

	/* initialize context for tree walk */
	context.pstate = pstate;
	context.collation = InvalidOid;
	context.strength = COLLATE_NONE;
	context.location = -1;

	/* and away we go */
	(void) assign_collations_walker(expr, &context);
};

bool
CollationParser::assign_collations_walker(PGNode *node, assign_collations_context *context)
{
    assign_collations_context loccontext;
	Oid			collation;
	CollateStrength strength;
	int			location;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return false;

	/*
	 * Prepare for recursion.  For most node types, though not all, the first
	 * thing we do is recurse to process all nodes below this one. Each level
	 * of the tree has its own local context.
	 */
	loccontext.pstate = context->pstate;
	loccontext.collation = InvalidOid;
	loccontext.strength = COLLATE_NONE;
	loccontext.location = -1;
	/* Set these fields just to suppress uninitialized-value warnings: */
	loccontext.collation2 = InvalidOid;
	loccontext.location2 = -1;

	/*
	 * Recurse if appropriate, then determine the collation for this node.
	 *
	 * Note: the general cases are at the bottom of the switch, after various
	 * special cases.
	 */
	switch (nodeTag(node))
	{
		case T_PGCollateExpr:
			{
				/*
				 * COLLATE sets an explicitly derived collation, regardless of
				 * what the child state is.  But we must recurse to set up
				 * collation info below here.
				 */
				PGCollateExpr *expr = (PGCollateExpr *) node;

				(void) expression_tree_walker(node,
											  assign_collations_walker,
											  (void *) &loccontext);

				collation = expr->collOid;
				Assert(OidIsValid(collation));
				strength = COLLATE_EXPLICIT;
				location = expr->location;
			}
			break;
		case T_PGFieldSelect:
			{
				/*
				 * For FieldSelect, the result has the field's declared
				 * collation, independently of what happened in the arguments.
				 * (The immediate argument must be composite and thus not
				 * collatable, anyhow.)  The field's collation was already
				 * looked up and saved in the node.
				 */
				PGFieldSelect *expr = (PGFieldSelect *) node;

				/* ... but first, recurse */
				(void) expression_tree_walker(node,
											  assign_collations_walker,
											  (void *) &loccontext);

				if (OidIsValid(expr->resultcollid))
				{
					/* Node's result type is collatable. */
					/* Pass up field's collation as an implicit choice. */
					collation = expr->resultcollid;
					strength = COLLATE_IMPLICIT;
					location = exprLocation(node);
				}
				else
				{
					/* Node's result type isn't collatable. */
					collation = InvalidOid;
					strength = COLLATE_NONE;
					location = -1;		/* won't be used */
				}
			}
			break;
		case T_PGRowExpr:
			{
				/*
				 * RowExpr is a special case because the subexpressions are
				 * independent: we don't want to complain if some of them have
				 * incompatible explicit collations.
				 */
				PGRowExpr    *expr = (PGRowExpr *) node;

				assign_list_collations(context->pstate, expr->args);

				/*
				 * Since the result is always composite and therefore never
				 * has a collation, we can just stop here: this node has no
				 * impact on the collation of its parent.
				 */
				return false;	/* done */
			}
		case T_PGRowCompareExpr:
			{
				/*
				 * For RowCompare, we have to find the common collation of
				 * each pair of input columns and build a list.  If we can't
				 * find a common collation, we just put InvalidOid into the
				 * list, which may or may not cause an error at runtime.
				 */
				PGRowCompareExpr *expr = (PGRowCompareExpr *) node;
				PGList	   *colls = NIL;
				ListCell   *l;
				ListCell   *r;

				forboth(l, expr->largs, r, expr->rargs)
				{
					PGNode	   *le = (PGNode *) lfirst(l);
					PGNode	   *re = (PGNode *) lfirst(r);
					Oid			coll;

					coll = select_common_collation(context->pstate,
												   list_make2(le, re),
												   true);
					colls = lappend_oid(colls, coll);
				}
				expr->inputcollids = colls;

				/*
				 * Since the result is always boolean and therefore never has
				 * a collation, we can just stop here: this node has no impact
				 * on the collation of its parent.
				 */
				return false;	/* done */
			}
		case T_PGCoerceToDomain:
			{
				/*
				 * If the domain declaration included a non-default COLLATE
				 * spec, then use that collation as the output collation of
				 * the coercion.  Otherwise allow the input collation to
				 * bubble up.  (The input should be of the domain's base type,
				 * therefore we don't need to worry about it not being
				 * collatable when the domain is.)
				 */
				PGCoerceToDomain *expr = (PGCoerceToDomain *) node;
				Oid			typcollation = get_typcollation(expr->resulttype);

				/* ... but first, recurse */
				(void) expression_tree_walker(node,
											  assign_collations_walker,
											  (void *) &loccontext);

				if (OidIsValid(typcollation))
				{
					/* Node's result type is collatable. */
					if (typcollation == DEFAULT_COLLATION_OID)
					{
						/* Collation state bubbles up from child. */
						collation = loccontext.collation;
						strength = loccontext.strength;
						location = loccontext.location;
					}
					else
					{
						/* Use domain's collation as an implicit choice. */
						collation = typcollation;
						strength = COLLATE_IMPLICIT;
						location = exprLocation(node);
					}
				}
				else
				{
					/* Node's result type isn't collatable. */
					collation = InvalidOid;
					strength = COLLATE_NONE;
					location = -1;		/* won't be used */
				}

				/*
				 * Save the state into the expression node.  We know it
				 * doesn't care about input collation.
				 */
				if (strength == COLLATE_CONFLICT)
					exprSetCollation(node, InvalidOid);
				else
					exprSetCollation(node, collation);
			}
			break;
		case T_PGTargetEntry:
			(void) expression_tree_walker(node,
										  assign_collations_walker,
										  (void *) &loccontext);

			/*
			 * TargetEntry can have only one child, and should bubble that
			 * state up to its parent.  We can't use the general-case code
			 * below because exprType and friends don't work on TargetEntry.
			 */
			collation = loccontext.collation;
			strength = loccontext.strength;
			location = loccontext.location;

			/*
			 * Throw error if the collation is indeterminate for a TargetEntry
			 * that is a sort/group target.  We prefer to do this now, instead
			 * of leaving the comparison functions to fail at runtime, because
			 * we can give a syntax error pointer to help locate the problem.
			 * There are some cases where there might not be a failure, for
			 * example if the planner chooses to use hash aggregation instead
			 * of sorting for grouping; but it seems better to predictably
			 * throw an error.  (Compare transformSetOperationTree, which will
			 * throw error for indeterminate collation of set-op columns, even
			 * though the planner might be able to implement the set-op
			 * without sorting.)
			 */
			if (strength == COLLATE_CONFLICT &&
				((PGTargetEntry *) node)->ressortgroupref != 0)
				ereport(ERROR,
						(errcode(ERRCODE_COLLATION_MISMATCH),
						 errmsg("collation mismatch between implicit collations \"%s\" and \"%s\"",
								get_collation_name(loccontext.collation),
								get_collation_name(loccontext.collation2)),
						 errhint("You can choose the collation by applying the COLLATE clause to one or both expressions."),
						 parser_errposition(context->pstate,
											loccontext.location2)));
			break;
		case T_PGRangeTblRef:
		case T_PGJoinExpr:
		case T_PGFromExpr:
		case T_PGSortGroupClause:
		//case T_PGGroupingClause:
		case T_PGWindowClause:
			(void) expression_tree_walker(node,
										  assign_collations_walker,
										  (void *) &loccontext);

			/*
			 * When we're invoked on a query's jointree, we don't need to do
			 * anything with join nodes except recurse through them to process
			 * WHERE/ON expressions.  So just stop here.  Likewise, we don't
			 * need to do anything when invoked on sort/group lists.
			 *
			 * GPDB: same for WindowClauses.
			 */
			return false;
		case T_PGQuery:
			{
				/*
				 * We get here when we're invoked on the Query belonging to a
				 * SubLink.  Act as though the Query returns its first output
				 * column, which indeed is what it does for EXPR_SUBLINK and
				 * ARRAY_SUBLINK cases.  In the cases where the SubLink
				 * returns boolean, this info will be ignored.  Special case:
				 * in EXISTS, the Query might return no columns, in which case
				 * we need do nothing.
				 *
				 * We needn't recurse, since the Query is already processed.
				 */
				PGQuery	   *qtree = (PGQuery *) node;
				PGTargetEntry *tent;

				if (qtree->targetList == NIL)
					return false;
				tent = (PGTargetEntry *) linitial(qtree->targetList);
				Assert(IsA(tent, PGTargetEntry));
				if (tent->resjunk)
					return false;

				collation = exprCollation((PGNode *) tent->expr);
				/* collation doesn't change if it's converted to array */
				strength = COLLATE_IMPLICIT;
				location = exprLocation((PGNode *) tent->expr);
			}
			break;
		case T_PGList:
			(void) expression_tree_walker(node,
										  assign_collations_walker,
										  (void *) &loccontext);

			/*
			 * When processing a list, collation state just bubbles up from
			 * the list elements.
			 */
			collation = loccontext.collation;
			strength = loccontext.strength;
			location = loccontext.location;
			break;

		case T_PGVar:
		case T_PGConst:
		case T_PGParam:
		case T_PGCoerceToDomainValue:
		case T_PGCaseTestExpr:
		case T_PGSetToDefault:
		case T_PGCurrentOfExpr:

			/*
			 * General case for childless expression nodes.  These should
			 * already have a collation assigned; it is not this function's
			 * responsibility to look into the catalogs for base-case
			 * information.
			 */
			collation = exprCollation(node);

			/*
			 * Note: in most cases, there will be an assigned collation
			 * whenever type_is_collatable(exprType(node)); but an exception
			 * occurs for a Var referencing a subquery output column for which
			 * a unique collation was not determinable.  That may lead to a
			 * runtime failure if a collation-sensitive function is applied to
			 * the Var.
			 */

			if (OidIsValid(collation))
				strength = COLLATE_IMPLICIT;
			else
				strength = COLLATE_NONE;
			location = exprLocation(node);
			break;

		default:
			{
				/*
				 * General case for most expression nodes with children. First
				 * recurse, then figure out what to assign to this node.
				 */
				Oid			typcollation;

				/*
				 * For most node types, we want to treat all the child
				 * expressions alike; but there are a few exceptions, hence
				 * this inner switch.
				 */
				switch (nodeTag(node))
				{
					case T_PGAggref:
						{
							/*
							 * Aggref is messy enough that we give it its own
							 * function, in fact three of them.  The FILTER
							 * clause is independent of the rest of the
							 * aggregate, however, so it can be processed
							 * separately.
							 */
							PGAggref	   *aggref = (PGAggref *) node;

							switch (aggref->aggkind)
							{
								case AGGKIND_NORMAL:
									assign_aggregate_collations(aggref,
																&loccontext);
									break;
								case AGGKIND_ORDERED_SET:
									assign_ordered_set_collations(aggref,
																&loccontext);
									break;
								case AGGKIND_HYPOTHETICAL:
									assign_hypothetical_collations(aggref,
																&loccontext);
									break;
								default:
									elog(ERROR, "unrecognized aggkind: %d",
										 (int) aggref->aggkind);
							}

							assign_expr_collations(context->pstate,
												 (PGNode *) aggref->aggfilter);
						}
						break;
					case T_PGWindowFunc:
						{
							/*
							 * WindowFunc requires special processing only for
							 * its aggfilter clause, as for aggregates.
							 */
							PGWindowFunc *wfunc = (PGWindowFunc *) node;

							(void) assign_collations_walker((PGNode *) wfunc->args,
															&loccontext);

							assign_expr_collations(context->pstate,
												   (PGNode *) wfunc->aggfilter);
						}
						break;
					case T_PGCaseExpr:
						{
							/*
							 * CaseExpr is a special case because we do not
							 * want to recurse into the test expression (if
							 * any).  It was already marked with collations
							 * during transformCaseExpr, and furthermore its
							 * collation is not relevant to the result of the
							 * CASE --- only the output expressions are.
							 */
							PGCaseExpr   *expr = (PGCaseExpr *) node;
							ListCell   *lc;

							foreach(lc, expr->args)
							{
								PGCaseWhen   *when = (PGCaseWhen *) lfirst(lc);

								Assert(IsA(when, PGCaseWhen));

								/*
								 * The condition expressions mustn't affect
								 * the CASE's result collation either; but
								 * since they are known to yield boolean, it's
								 * safe to recurse directly on them --- they
								 * won't change loccontext.
								 */
								(void) assign_collations_walker((PGNode *) when->expr,
																&loccontext);
								(void) assign_collations_walker((PGNode *) when->result,
																&loccontext);
							}
							(void) assign_collations_walker((PGNode *) expr->defresult,
															&loccontext);
						}
						break;
					default:

						/*
						 * Normal case: all child expressions contribute
						 * equally to loccontext.
						 */
						(void) expression_tree_walker(node,
													assign_collations_walker,
													  (void *) &loccontext);
						break;
				}

				/*
				 * Now figure out what collation to assign to this node.
				 */
				typcollation = get_typcollation(exprType(node));
				if (OidIsValid(typcollation))
				{
					/* Node's result is collatable; what about its input? */
					if (loccontext.strength > COLLATE_NONE)
					{
						/* Collation state bubbles up from children. */
						collation = loccontext.collation;
						strength = loccontext.strength;
						location = loccontext.location;
					}
					else
					{
						/*
						 * Collatable output produced without any collatable
						 * input.  Use the type's collation (which is usually
						 * DEFAULT_COLLATION_OID, but might be different for a
						 * domain).
						 */
						collation = typcollation;
						strength = COLLATE_IMPLICIT;
						location = exprLocation(node);
					}
				}
				else
				{
					/* Node's result type isn't collatable. */
					collation = InvalidOid;
					strength = COLLATE_NONE;
					location = -1;		/* won't be used */
				}

				/*
				 * Save the result collation into the expression node. If the
				 * state is COLLATE_CONFLICT, we'll set the collation to
				 * InvalidOid, which might result in an error at runtime.
				 */
				if (strength == COLLATE_CONFLICT)
					exprSetCollation(node, InvalidOid);
				else
					exprSetCollation(node, collation);

				/*
				 * Likewise save the input collation, which is the one that
				 * any function called by this node should use.
				 */
				if (loccontext.strength == COLLATE_CONFLICT)
					exprSetInputCollation(node, InvalidOid);
				else
					exprSetInputCollation(node, loccontext.collation);
			}
			break;
	}

	/*
	 * Now, merge my information into my parent's state.
	 */
	merge_collation_state(collation,
						  strength,
						  location,
						  loccontext.collation2,
						  loccontext.location2,
						  context);

	return false;
};

void
CollationParser::assign_aggregate_collations(PGAggref *aggref,
							assign_collations_context *loccontext)
{
    PGListCell   *lc;

	/* Plain aggregates have no direct args */
	Assert(aggref->aggdirectargs == NIL);

	/* Process aggregated args, holding resjunk ones at arm's length */
	foreach(lc, aggref->args)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(lc);

		Assert(IsA(tle, PGTargetEntry));
		if (tle->resjunk)
			assign_expr_collations(loccontext->pstate, (PGNode *) tle);
		else
			(void) assign_collations_walker((PGNode *) tle, loccontext);
	}
};

void
CollationParser::assign_ordered_set_collations(PGAggref *aggref,
							  assign_collations_context *loccontext)
{
    bool		merge_sort_collations;
	PGListCell   *lc;

	/* Merge sort collations to parent only if there can be only one */
	merge_sort_collations = (list_length(aggref->args) == 1 &&
					  get_func_variadictype(aggref->aggfnoid) == InvalidOid);

	/* Direct args, if any, are normal children of the Aggref node */
	(void) assign_collations_walker((PGNode *) aggref->aggdirectargs,
									loccontext);

	/* Process aggregated args appropriately */
	foreach(lc, aggref->args)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(lc);

		Assert(IsA(tle, PGTargetEntry));
		if (merge_sort_collations)
			(void) assign_collations_walker((PGNode *) tle, loccontext);
		else
			assign_expr_collations(loccontext->pstate, (PGNode *) tle);
	}
};

void
CollationParser::assign_hypothetical_collations(PGAggref *aggref,
							   assign_collations_context *loccontext)
{
    PGListCell   *h_cell = list_head(aggref->aggdirectargs);
	PGListCell   *s_cell = list_head(aggref->args);
	bool		merge_sort_collations;
	int			extra_args;

	/* Merge sort collations to parent only if there can be only one */
	merge_sort_collations = (list_length(aggref->args) == 1 &&
					  get_func_variadictype(aggref->aggfnoid) == InvalidOid);

	/* Process any non-hypothetical direct args */
	extra_args = list_length(aggref->aggdirectargs) - list_length(aggref->args);
	Assert(extra_args >= 0);
	while (extra_args-- > 0)
	{
		(void) assign_collations_walker((PGNode *) lfirst(h_cell), loccontext);
		h_cell = lnext(h_cell);
	}

	/* Scan hypothetical args and aggregated args in parallel */
	while (h_cell && s_cell)
	{
		PGNode	   *h_arg = (PGNode *) lfirst(h_cell);
		PGTargetEntry *s_tle = (PGTargetEntry *) lfirst(s_cell);
		assign_collations_context paircontext;

		/*
		 * Assign collations internally in this pair of expressions, then
		 * choose a common collation for them.  This should match
		 * select_common_collation(), but we can't use that function as-is
		 * because we need access to the whole collation state so we can
		 * bubble it up to the aggregate function's level.
		 */
		paircontext.pstate = loccontext->pstate;
		paircontext.collation = InvalidOid;
		paircontext.strength = COLLATE_NONE;
		paircontext.location = -1;
		/* Set these fields just to suppress uninitialized-value warnings: */
		paircontext.collation2 = InvalidOid;
		paircontext.location2 = -1;

		(void) assign_collations_walker(h_arg, &paircontext);
		(void) assign_collations_walker((PGNode *) s_tle->expr, &paircontext);

		/* deal with collation conflict */
		if (paircontext.strength == COLLATE_CONFLICT)
			ereport(ERROR,
					(errcode(ERRCODE_COLLATION_MISMATCH),
					 errmsg("collation mismatch between implicit collations \"%s\" and \"%s\"",
							get_collation_name(paircontext.collation),
							get_collation_name(paircontext.collation2)),
					 errhint("You can choose the collation by applying the COLLATE clause to one or both expressions."),
					 parser_errposition(paircontext.pstate,
										paircontext.location2)));

		/*
		 * At this point paircontext.collation can be InvalidOid only if the
		 * type is not collatable; no need to do anything in that case.  If we
		 * do have to change the sort column's collation, do it by inserting a
		 * RelabelType node into the sort column TLE.
		 *
		 * XXX This is pretty grotty for a couple of reasons:
		 * assign_collations_walker isn't supposed to be changing the
		 * expression structure like this, and a parse-time change of
		 * collation ought to be signaled by a CollateExpr not a RelabelType
		 * (the use of RelabelType for collation marking is supposed to be a
		 * planner/executor thing only).  But we have no better alternative.
		 * In particular, injecting a CollateExpr could result in the
		 * expression being interpreted differently after dump/reload, since
		 * we might be effectively promoting an implicit collation to
		 * explicit.  This kluge is relying on ruleutils.c not printing a
		 * COLLATE clause for a RelabelType, and probably on some other
		 * fragile behaviors.
		 */
		if (OidIsValid(paircontext.collation) &&
			paircontext.collation != exprCollation((PGNode *) s_tle->expr))
		{
			s_tle->expr = (PGExpr *)
				makeRelabelType(s_tle->expr,
								exprType((PGNode *) s_tle->expr),
								exprTypmod((PGNode *) s_tle->expr),
								paircontext.collation,
								PG_COERCE_IMPLICIT_CAST);
		}

		/*
		 * If appropriate, merge this column's collation state up to the
		 * aggregate function.
		 */
		if (merge_sort_collations)
			merge_collation_state(paircontext.collation,
								  paircontext.strength,
								  paircontext.location,
								  paircontext.collation2,
								  paircontext.location2,
								  loccontext);

		h_cell = lnext(h_cell);
		s_cell = lnext(s_cell);
	}
	Assert(h_cell == NULL && s_cell == NULL);
};

void
CollationParser::merge_collation_state(Oid collation,
					  CollateStrength strength,
					  int location,
					  Oid collation2,
					  int location2,
					  assign_collations_context *context)
{
    /*
	 * If the collation strength for this node is different from what's
	 * already in *context, then this node either dominates or is dominated by
	 * earlier siblings.
	 */
	if (strength > context->strength)
	{
		/* Override previous parent state */
		context->collation = collation;
		context->strength = strength;
		context->location = location;
		/* Bubble up error info if applicable */
		if (strength == COLLATE_CONFLICT)
		{
			context->collation2 = collation2;
			context->location2 = location2;
		}
	}
	else if (strength == context->strength)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (strength)
		{
			case COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case COLLATE_IMPLICIT:
				if (collation != context->collation)
				{
					/*
					 * Non-default implicit collation always beats default.
					 */
					if (context->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						context->collation = collation;
						context->strength = strength;
						context->location = location;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Ooops, we have a conflict.  We cannot throw error
						 * here, since the conflict could be resolved by a
						 * later sibling CollateExpr, or the parent might not
						 * care about collation anyway.  Return enough info to
						 * throw the error later, if needed.
						 */
						context->strength = COLLATE_CONFLICT;
						context->collation2 = collation;
						context->location2 = location;
					}
				}
				break;
			case COLLATE_CONFLICT:
				/* We're still conflicted ... */
				break;
			case COLLATE_EXPLICIT:
				if (collation != context->collation)
				{
					/*
					 * Ooops, we have a conflict of explicit COLLATE clauses.
					 * Here we choose to throw error immediately; that is what
					 * the SQL standard says to do, and there's no good reason
					 * to be less strict.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_COLLATION_MISMATCH),
							 errmsg("collation mismatch between explicit collations \"%s\" and \"%s\"",
									get_collation_name(context->collation),
									get_collation_name(collation)),
							 parser_errposition(context->pstate, location)));
				}
				break;
		}
	}
};

bool
CollationParser::expression_tree_walker(PGNode *node,
					   walker_func walker,
					   void *context)
{
    auto context_ptr = reinterpret_cast<assign_collations_context*>(context);
    PGListCell   *temp;

	/*
	 * The walker has already visited the current node, and so we need only
	 * recurse into any sub-nodes it has.
	 *
	 * We assume that the walker is not interested in List nodes per se, so
	 * when we expect a List we just recurse directly to self without
	 * bothering to call the walker.
	 */
	if (node == NULL)
		return false;

	/* Guard against stack overflow due to overly complex expressions */
	//check_stack_depth();

	switch (nodeTag(node))
	{
		case T_PGVar:
		case T_PGConst:
		case T_PGParam:
		case T_PGCoerceToDomainValue:
		case T_PGCaseTestExpr:
		case T_PGSetToDefault:
		case T_PGCurrentOfExpr:
		case T_PGRangeTblRef:
		case T_PGSortGroupClause:
		// case T_PGDMLActionExpr:
		// case T_PGPartSelectedExpr:
		// case T_PGPartDefaultExpr:
		// case T_PGPartBoundExpr:
		// case T_PGPartBoundInclusionExpr:
		// case T_PGPartBoundOpenExpr:
		// case T_PGPartListRuleExpr:
		// case T_PGPartListNullTestExpr:
			/* primitive node types with no expression subnodes */
			break;
		// case T_PGWithCheckOption:
		// 	return walker(((PGWithCheckOption *) node)->qual, context);
		case T_PGAggref:
			{
				PGAggref	   *expr = (PGAggref *) node;

				/* recurse directly on List */
				if (expression_tree_walker((PGNode *) expr->aggdirectargs,
										   walker, context))
					return true;
				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
				if (expression_tree_walker((PGNode *) expr->aggorder,
										   walker, context))
					return true;
				if (expression_tree_walker((PGNode *) expr->aggdistinct,
										   walker, context))
					return true;
				if ((this->*walker)((PGNode *) expr->aggfilter, context_ptr))
					return true;
			}
			break;
		case T_PGWindowFunc:
			{
				PGWindowFunc   *expr = (PGWindowFunc *) node;

				/* recurse directly on explicit arg List */
				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
				if ((this->*walker)((PGNode *) expr->aggfilter, context_ptr))
					return true;
			}
			break;
		case T_PGArrayRef:
			{
				PGArrayRef   *aref = (PGArrayRef *) node;

				/* recurse directly for upper/lower array index lists */
				if (expression_tree_walker((PGNode *) aref->refupperindexpr,
										   walker, context))
					return true;
				if (expression_tree_walker((PGNode *) aref->reflowerindexpr,
										   walker, context))
					return true;
				/* walker must see the refexpr and refassgnexpr, however */
				if ((this->*walker)((PGNode *)aref->refexpr, context_ptr))
					return true;
				if ((this->*walker)((PGNode *)aref->refassgnexpr, context_ptr))
					return true;
			}
			break;
		case T_PGFuncExpr:
			{
				PGFuncExpr   *expr = (PGFuncExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGNamedArgExpr:
			return (this->*walker)((PGNode *)((PGNamedArgExpr *) node)->arg, context_ptr);
		case T_PGOpExpr:
		case T_PGDistinctExpr:	/* struct-equivalent to OpExpr */
		case T_PGNullIfExpr:		/* struct-equivalent to OpExpr */
			{
				PGOpExpr	   *expr = (PGOpExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGScalarArrayOpExpr:
			{
				PGScalarArrayOpExpr *expr = (PGScalarArrayOpExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGBoolExpr:
			{
				PGBoolExpr   *expr = (PGBoolExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGSubLink:
			{
				PGSubLink    *sublink = (PGSubLink *) node;

				if ((this->*walker)(sublink->testexpr, context_ptr))
					return true;

				/*
				 * Also invoke the walker on the sublink's Query node, so it
				 * can recurse into the sub-query if it wants to.
				 */
				return (this->*walker)(sublink->subselect, context_ptr);
			}
			break;
		case T_PGSubPlan:
			{
				PGSubPlan    *subplan = (PGSubPlan *) node;

				/* recurse into the testexpr, but not into the Plan */
				if ((this->*walker)(subplan->testexpr, context_ptr))
					return true;
				/* also examine args list */
				if (expression_tree_walker((PGNode *) subplan->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGAlternativeSubPlan:
			return (this->*walker)((PGNode *)((PGAlternativeSubPlan *) node)->subplans, context_ptr);
		case T_PGFieldSelect:
			return (this->*walker)((PGNode *)((PGFieldSelect *) node)->arg, context_ptr);
		case T_PGFieldStore:
			{
				PGFieldStore *fstore = (PGFieldStore *) node;

				if ((this->*walker)((PGNode *)fstore->arg, context_ptr))
					return true;
				if ((this->*walker)((PGNode *)fstore->newvals, context_ptr))
					return true;
			}
			break;
		case T_PGRelabelType:
			return (this->*walker)((PGNode *)((PGRelabelType *) node)->arg, context_ptr);
		case T_PGCoerceViaIO:
			return (this->*walker)((PGNode *)((PGCoerceViaIO *) node)->arg, context_ptr);
		case T_PGArrayCoerceExpr:
			return (this->*walker)((PGNode *)((PGArrayCoerceExpr *) node)->arg, context_ptr);
		case T_PGConvertRowtypeExpr:
			return (this->*walker)((PGNode *)((PGConvertRowtypeExpr *) node)->arg, context_ptr);
		case T_PGCollateExpr:
			return (this->*walker)((PGNode *)((PGCollateExpr *) node)->arg, context_ptr);
		case T_PGCaseExpr:
			{
				PGCaseExpr   *caseexpr = (PGCaseExpr *) node;

				if ((this->*walker)((PGNode *)caseexpr->arg, context_ptr))
					return true;
				/* we assume walker doesn't care about CaseWhens, either */
				foreach(temp, caseexpr->args)
				{
					PGCaseWhen   *when = (PGCaseWhen *) lfirst(temp);

					Assert(IsA(when, PGCaseWhen));
					if ((this->*walker)((PGNode *)when->expr, context_ptr))
						return true;
					if ((this->*walker)((PGNode *)when->result, context_ptr))
						return true;
				}
				if ((this->*walker)((PGNode *)caseexpr->defresult, context_ptr))
					return true;
			}
			break;
		case T_PGArrayExpr:
			return (this->*walker)((PGNode *)((PGArrayExpr *) node)->elements, context_ptr);
		case T_PGRowExpr:
			/* Assume colnames isn't interesting */
			return (this->*walker)((PGNode *)((PGRowExpr *) node)->args, context_ptr);
		case T_PGRowCompareExpr:
			{
				PGRowCompareExpr *rcexpr = (PGRowCompareExpr *) node;

				if ((this->*walker)((PGNode *)rcexpr->largs, context_ptr))
					return true;
				if ((this->*walker)((PGNode *)rcexpr->rargs, context_ptr))
					return true;
			}
			break;
		case T_PGCoalesceExpr:
			return (this->*walker)((PGNode *)((PGCoalesceExpr *) node)->args, context_ptr);
		case T_PGMinMaxExpr:
			return (this->*walker)((PGNode *)((PGMinMaxExpr *) node)->args, context_ptr);
		// case T_PGXmlExpr:
		// 	{
		// 		PGXmlExpr    *xexpr = (PGXmlExpr *) node;

		// 		if (walker(xexpr->named_args, context))
		// 			return true;
		// 		/* we assume walker doesn't care about arg_names */
		// 		if (walker(xexpr->args, context))
		// 			return true;
		// 	}
		// 	break;
		case T_PGNullTest:
			return (this->*walker)((PGNode *)((PGNullTest *) node)->arg, context_ptr);
		case T_PGBooleanTest:
			return (this->*walker)((PGNode *)((PGBooleanTest *) node)->arg, context_ptr);
		case T_PGCoerceToDomain:
			return (this->*walker)((PGNode *)((PGCoerceToDomain *) node)->arg, context_ptr);
		case T_PGTargetEntry:
			return (this->*walker)((PGNode *)((PGTargetEntry *) node)->expr, context_ptr);
		case T_PGQuery:
			/* Do nothing with a sub-Query, per discussion above */
			break;
		case T_PGCommonTableExpr:
			{
				PGCommonTableExpr *cte = (PGCommonTableExpr *) node;

				/*
				 * Invoke the walker on the CTE's Query node, so it can
				 * recurse into the sub-query if it wants to.
				 */
				return (this->*walker)(cte->ctequery, context_ptr);
			}
			break;
		case T_PGList:
			foreach(temp, (PGList *) node)
			{
				if ((this->*walker)((PGNode *) lfirst(temp), context_ptr))
					return true;
			}
			break;
		case T_PGFromExpr:
			{
				PGFromExpr   *from = (PGFromExpr *) node;

				if ((this->*walker)((PGNode *)from->fromlist, context_ptr))
					return true;
				if ((this->*walker)(from->quals, context_ptr))
					return true;
			}
			break;
		case T_PGJoinExpr:
			{
				PGJoinExpr   *join = (PGJoinExpr *) node;

				if ((this->*walker)(join->larg, context_ptr))
					return true;
				if ((this->*walker)(join->rarg, context_ptr))
					return true;
				if ((this->*walker)(join->quals, context_ptr))
					return true;

				/*
				 * alias clause, using list are deemed uninteresting.
				 */
			}
			break;
		// case T_PGSetOperationStmt:
		// 	{
		// 		PGSetOperationStmt *setop = (PGSetOperationStmt *) node;

		// 		if (walker(setop->larg, context))
		// 			return true;
		// 		if (walker(setop->rarg, context))
		// 			return true;

		// 		/* groupClauses are deemed uninteresting */
		// 	}
		// 	break;
		// case T_PGPlaceHolderVar:
		// 	return walker(((PGPlaceHolderVar *) node)->phexpr, context);
		// case T_PGAppendRelInfo:
		// 	{
		// 		PGAppendRelInfo *appinfo = (PGAppendRelInfo *) node;

		// 		if (expression_tree_walker((PGNode *) appinfo->translated_vars,
		// 								   walker, context))
		// 			return true;
		// 	}
		// 	break;
		// case T_PGPlaceHolderInfo:
		// 	return walker(((PGPlaceHolderInfo *) node)->ph_var, context);
		case T_PGRangeTblFunction:
			return (this->*walker)(((PGRangeTblFunction *) node)->funcexpr, context_ptr);

		// case T_PGGroupingClause:
		// 	{
		// 		PGGroupingClause *g = (PGGroupingClause *) node;
		// 		if (expression_tree_walker((PGNode *)g->groupsets, walker,
		// 			context))
		// 			return true;
		// 	}
		// 	break;
		case T_PGGroupingFunc:
			break;
		// case T_PGGrouping:
		// case T_PGGroupId:
		// 	{
		// 		/* do nothing */
		// 	}
		// 	break;
		case T_PGWindowDef:
			{
				PGWindowDef  *wd = (PGWindowDef *) node;

				if (expression_tree_walker((PGNode *) wd->partitionClause, walker,
										   context))
					return true;
				if (expression_tree_walker((PGNode *) wd->orderClause, walker,
										   context))
					return true;
				if ((this->*walker)((PGNode *) wd->startOffset, context_ptr))
					return true;
				if ((this->*walker)((PGNode *) wd->endOffset, context_ptr))
					return true;
			}
			break;
		case T_PGTypeCast:
			{
				PGTypeCast *tc = (PGTypeCast *)node;

				if (expression_tree_walker((PGNode*) tc->arg, walker, context))
					return true;
			}
			break;
		// case T_PGTableValueExpr:
		// 	{
		// 		PGTableValueExpr *expr = (PGTableValueExpr *) node;

		// 		return walker(expr->subquery, context);
		// 	}
		// 	break;
		case T_PGWindowClause:
			{
				PGWindowClause *wc = (PGWindowClause *) node;

				if (expression_tree_walker((PGNode *) wc->partitionClause, walker,
										   context))
					return true;
				if (expression_tree_walker((PGNode *) wc->orderClause, walker,
										   context))
					return true;
				if ((this->*walker)((PGNode *) wc->startOffset, context_ptr))
					return true;
				if ((this->*walker)((PGNode *) wc->endOffset, context_ptr))
					return true;
				return false;
			}
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
	return false;
};

}