#include <Interpreters/orcaopt/pgoptnew/CollationParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

void
pg_assign_aggregate_collations(PGAggref *aggref,
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
			pg_assign_expr_collations(loccontext->pstate, (PGNode *) tle);
		else
			(void) pg_assign_collations_walker((PGNode *) tle, loccontext);
	}
};

void
pg_assign_ordered_set_collations(PGAggref *aggref,
							  assign_collations_context *loccontext)
{
	bool		merge_sort_collations;
	PGListCell   *lc;

	/* Merge sort collations to parent only if there can be only one */
	merge_sort_collations = (list_length(aggref->args) == 1 &&
					  get_func_variadictype(aggref->aggfnoid) == InvalidOid);

	/* Direct args, if any, are normal children of the Aggref node */
	(void) pg_assign_collations_walker((Node *) aggref->aggdirectargs,
									loccontext);

	/* Process aggregated args appropriately */
	foreach(lc, aggref->args)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(lc);

		Assert(IsA(tle, PGTargetEntry));
		if (merge_sort_collations)
			(void) pg_assign_collations_walker((PGNode *) tle, loccontext);
		else
			pg_assign_expr_collations(loccontext->pstate, (PGNode *) tle);
	}
};

void
pg_merge_collation_state(Oid collation,
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

void
pg_assign_hypothetical_collations(PGAggref *aggref,
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
		(void) pg_assign_collations_walker((PGNode *) lfirst(h_cell), loccontext);
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

		(void) pg_assign_collations_walker(h_arg, &paircontext);
		(void) pg_assign_collations_walker((PGNode *) s_tle->expr, &paircontext);

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
								COERCE_IMPLICIT_CAST);
		}

		/*
		 * If appropriate, merge this column's collation state up to the
		 * aggregate function.
		 */
		if (merge_sort_collations)
			pg_merge_collation_state(paircontext.collation,
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

bool
pg_assign_collations_walker(PGNode *node, assign_collations_context *context)
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
		case T_CollateExpr:
			{
				/*
				 * COLLATE sets an explicitly derived collation, regardless of
				 * what the child state is.  But we must recurse to set up
				 * collation info below here.
				 */
				CollateExpr *expr = (CollateExpr *) node;

				(void) pg_expression_tree_walker(node,
											  assign_collations_walker,
											  (void *) &loccontext);

				collation = expr->collOid;
				Assert(OidIsValid(collation));
				strength = COLLATE_EXPLICIT;
				location = expr->location;
			}
			break;
		case T_FieldSelect:
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
				(void) pg_expression_tree_walker(node,
											  pg_assign_collations_walker,
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
		case T_RowExpr:
			{
				/*
				 * RowExpr is a special case because the subexpressions are
				 * independent: we don't want to complain if some of them have
				 * incompatible explicit collations.
				 */
				RowExpr    *expr = (RowExpr *) node;

				pg_assign_list_collations(context->pstate, expr->args);

				/*
				 * Since the result is always composite and therefore never
				 * has a collation, we can just stop here: this node has no
				 * impact on the collation of its parent.
				 */
				return false;	/* done */
			}
		case T_RowCompareExpr:
			{
				/*
				 * For RowCompare, we have to find the common collation of
				 * each pair of input columns and build a list.  If we can't
				 * find a common collation, we just put InvalidOid into the
				 * list, which may or may not cause an error at runtime.
				 */
				PGRowCompareExpr *expr = (PGRowCompareExpr *) node;
				PGList	   *colls = NIL;
				PGListCell   *l;
				PGListCell   *r;

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
		case T_CoerceToDomain:
			{
				/*
				 * If the domain declaration included a non-default COLLATE
				 * spec, then use that collation as the output collation of
				 * the coercion.  Otherwise allow the input collation to
				 * bubble up.  (The input should be of the domain's base type,
				 * therefore we don't need to worry about it not being
				 * collatable when the domain is.)
				 */
				CoerceToDomain *expr = (CoerceToDomain *) node;
				Oid			typcollation = get_typcollation(expr->resulttype);

				/* ... but first, recurse */
				(void) pg_expression_tree_walker(node,
											  pg_assign_collations_walker,
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
		case T_TargetEntry:
			(void) pg_expression_tree_walker(node,
										  pg_assign_collations_walker,
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
		case T_RangeTblRef:
		case T_JoinExpr:
		case T_FromExpr:
		case T_SortGroupClause:
		case T_GroupingClause:
		case T_WindowClause:
			(void) pg_expression_tree_walker(node,
										  pg_assign_collations_walker,
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
		case T_Query:
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
		case T_List:
			(void) pg_expression_tree_walker(node,
										  pg_assign_collations_walker,
										  (void *) &loccontext);

			/*
			 * When processing a list, collation state just bubbles up from
			 * the list elements.
			 */
			collation = loccontext.collation;
			strength = loccontext.strength;
			location = loccontext.location;
			break;

		case T_Var:
		case T_Const:
		case T_Param:
		case T_CoerceToDomainValue:
		case T_CaseTestExpr:
		case T_SetToDefault:
		case T_CurrentOfExpr:

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
					case T_Aggref:
						{
							/*
							 * Aggref is messy enough that we give it its own
							 * function, in fact three of them.  The FILTER
							 * clause is independent of the rest of the
							 * aggregate, however, so it can be processed
							 * separately.
							 */
							Aggref	   *aggref = (Aggref *) node;

							switch (aggref->aggkind)
							{
								case AGGKIND_NORMAL:
									pg_assign_aggregate_collations(aggref,
																&loccontext);
									break;
								case AGGKIND_ORDERED_SET:
									pg_assign_ordered_set_collations(aggref,
																&loccontext);
									break;
								case AGGKIND_HYPOTHETICAL:
									pg_assign_hypothetical_collations(aggref,
																&loccontext);
									break;
								default:
									elog(ERROR, "unrecognized aggkind: %d",
										 (int) aggref->aggkind);
							}

							pg_assign_expr_collations(context->pstate,
												 (Node *) aggref->aggfilter);
						}
						break;
					case T_WindowFunc:
						{
							/*
							 * WindowFunc requires special processing only for
							 * its aggfilter clause, as for aggregates.
							 */
							PGWindowFunc *wfunc = (PGWindowFunc *) node;

							(void) pg_assign_collations_walker((PGNode *) wfunc->args,
															&loccontext);

							pg_assign_expr_collations(context->pstate,
												   (PGNode *) wfunc->aggfilter);
						}
						break;
					case T_CaseExpr:
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
							PGListCell   *lc;

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
								(void) pg_assign_collations_walker((PGNode *) when->expr,
																&loccontext);
								(void) pg_assign_collations_walker((PGNode *) when->result,
																&loccontext);
							}
							(void) pg_assign_collations_walker((PGNode *) expr->defresult,
															&loccontext);
						}
						break;
					default:

						/*
						 * Normal case: all child expressions contribute
						 * equally to loccontext.
						 */
						(void) pg_expression_tree_walker(node,
													pg_assign_collations_walker,
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
	pg_merge_collation_state(collation,
						  strength,
						  location,
						  loccontext.collation2,
						  loccontext.location2,
						  context);

	return false;
};

void
pg_assign_expr_collations(PGParseState *pstate, PGNode *expr)
{
    assign_collations_context context;

	/* initialize context for tree walk */
	context.pstate = pstate;
	context.collation = InvalidOid;
	context.strength = COLLATE_NONE;
	context.location = -1;

	/* and away we go */
	(void) pg_assign_collations_walker(expr, &context);
};

void
pg_assign_list_collations(PGParseState *pstate, PGList *exprs)
{
	PGListCell   *lc;

	foreach(lc, exprs)
	{
		PGNode	   *node = (PGNode *) lfirst(lc);

		pg_assign_expr_collations(pstate, node);
	}
};

void
pg_assign_query_collations(PGParseState *pstate, PGQuery *query)
{
	/*
	 * We just use query_tree_walker() to visit all the contained expressions.
	 * We can skip the rangetable and CTE subqueries, though, since RTEs and
	 * subqueries had better have been processed already (else Vars referring
	 * to them would not get created with the right collation).
	 */
	(void) pg_query_tree_walker(query,
							 pg_assign_query_collations_walker,
							 (void *) pstate,
							 QTW_IGNORE_RANGE_TABLE |
							 QTW_IGNORE_CTE_SUBQUERIES);
};

bool
pg_assign_query_collations_walker(PGNode *node, PGParseState *pstate)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return false;

	/*
	 * We don't want to recurse into a set-operations tree; it's already been
	 * fully processed in transformSetOperationStmt.
	 */
	if (IsA(node, SetOperationStmt))
		return false;

	if (IsA(node, List))
		assign_list_collations(pstate, (List *) node);
	else
		pg_assign_expr_collations(pstate, node);

	return false;
};

}