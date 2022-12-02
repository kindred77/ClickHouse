#include <Interpreters/orcaopt/pgoptnew/walkers.h>

using namespace duckdb_libpgquery;

bool
pg_expression_tree_walker(PGNode *node,
					   walker_func walker,
					   void *context)
{
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
				if (pg_expression_tree_walker((PGNode *) expr->aggdirectargs,
										   walker, context))
					return true;
				if (pg_expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
				if (pg_expression_tree_walker((PGNode *) expr->aggorder,
										   walker, context))
					return true;
				if (pg_expression_tree_walker((PGNode *) expr->aggdistinct,
										   walker, context))
					return true;
				if (walker((PGNode *) expr->aggfilter, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGWindowFunc:
			{
				PGWindowFunc   *expr = (PGWindowFunc *) node;

				/* recurse directly on explicit arg List */
				if (pg_expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
				if (walker((PGNode *) expr->aggfilter, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGArrayRef:
			{
				PGArrayRef   *aref = (PGArrayRef *) node;

				/* recurse directly for upper/lower array index lists */
				if (pg_expression_tree_walker((PGNode *) aref->refupperindexpr,
										   walker, context))
					return true;
				if (pg_expression_tree_walker((PGNode *) aref->reflowerindexpr,
										   walker, context))
					return true;
				/* walker must see the refexpr and refassgnexpr, however */
				if (walker(reinterpret_cast<PGNode*>(aref->refexpr),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(reinterpret_cast<PGNode*>(aref->refassgnexpr),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGFuncExpr:
			{
				PGFuncExpr   *expr = (PGFuncExpr *) node;

				if (pg_expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGNamedArgExpr:
			return walker(reinterpret_cast<PGNode*>(((PGNamedArgExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGOpExpr:
		case T_PGDistinctExpr:	/* struct-equivalent to OpExpr */
		case T_PGNullIfExpr:		/* struct-equivalent to OpExpr */
			{
				PGOpExpr	   *expr = (PGOpExpr *) node;

				if (pg_expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGScalarArrayOpExpr:
			{
				PGScalarArrayOpExpr *expr = (PGScalarArrayOpExpr *) node;

				if (pg_expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGBoolExpr:
			{
				PGBoolExpr   *expr = (PGBoolExpr *) node;

				if (pg_expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGSubLink:
			{
				PGSubLink    *sublink = (PGSubLink *) node;

				if (walker(sublink->testexpr, reinterpret_cast<assign_collations_context*>(context)))
					return true;

				/*
				 * Also invoke the walker on the sublink's Query node, so it
				 * can recurse into the sub-query if it wants to.
				 */
				return walker(sublink->subselect, reinterpret_cast<assign_collations_context*>(context));
			}
			break;
		case T_PGSubPlan:
			{
				PGSubPlan    *subplan = (PGSubPlan *) node;

				/* recurse into the testexpr, but not into the Plan */
				if (walker(subplan->testexpr, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				/* also examine args list */
				if (pg_expression_tree_walker((PGNode *) subplan->args,
										   walker, context))
					return true;
			}
			break;
		case T_PGAlternativeSubPlan:
			return walker(reinterpret_cast<PGNode*>(((PGAlternativeSubPlan *) node)->subplans), 
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGFieldSelect:
			return walker(reinterpret_cast<PGNode*>(((PGFieldSelect *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGFieldStore:
			{
				PGFieldStore *fstore = (PGFieldStore *) node;

				if (walker(reinterpret_cast<PGNode*>(fstore->arg),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(reinterpret_cast<PGNode*>(fstore->newvals),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGRelabelType:
			return walker(reinterpret_cast<PGNode*>(((PGRelabelType *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGCoerceViaIO:
			return walker(reinterpret_cast<PGNode*>(((PGCoerceViaIO *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGArrayCoerceExpr:
			return walker(reinterpret_cast<PGNode*>(((PGArrayCoerceExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGConvertRowtypeExpr:
			return walker(reinterpret_cast<PGNode*>(((PGConvertRowtypeExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGCollateExpr:
			return walker(reinterpret_cast<PGNode*>(((PGCollateExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGCaseExpr:
			{
				PGCaseExpr   *caseexpr = (PGCaseExpr *) node;

				if (walker(reinterpret_cast<PGNode*>(caseexpr->arg), 
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
				/* we assume walker doesn't care about CaseWhens, either */
				foreach(temp, caseexpr->args)
				{
					PGCaseWhen   *when = (PGCaseWhen *) lfirst(temp);

					//Assert(IsA(when, PGCaseWhen));
                    Assert(nodeTag(when) == T_PGCaseExpr);
					if (walker(reinterpret_cast<PGNode*>(when->expr), reinterpret_cast<assign_collations_context*>(context)))
						return true;
					if (walker(reinterpret_cast<PGNode*>(when->result), reinterpret_cast<assign_collations_context*>(context)))
						return true;
				}
				if (walker(reinterpret_cast<PGNode*>(caseexpr->defresult), reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGArrayExpr:
			return walker(reinterpret_cast<PGNode*>(((PGArrayExpr *) node)->elements),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGRowExpr:
			/* Assume colnames isn't interesting */
			return walker(reinterpret_cast<PGNode*>(((PGRowExpr *) node)->args),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGRowCompareExpr:
			{
				PGRowCompareExpr *rcexpr = (PGRowCompareExpr *) node;

				if (walker(reinterpret_cast<PGNode*>(rcexpr->largs),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(reinterpret_cast<PGNode*>(rcexpr->rargs),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGCoalesceExpr:
			return walker(reinterpret_cast<PGNode*>(((PGCoalesceExpr *) node)->args),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGMinMaxExpr:
			return walker(reinterpret_cast<PGNode*>(((PGMinMaxExpr *) node)->args),
                reinterpret_cast<assign_collations_context*>(context));
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
			return walker(reinterpret_cast<PGNode*>(((PGNullTest *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGBooleanTest:
			return walker(reinterpret_cast<PGNode*>(((PGBooleanTest *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGCoerceToDomain:
			return walker(reinterpret_cast<PGNode*>(((PGCoerceToDomain *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case T_PGTargetEntry:
			return walker(reinterpret_cast<PGNode*>(((PGTargetEntry *) node)->expr),
                reinterpret_cast<assign_collations_context*>(context));
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
				return walker(cte->ctequery, reinterpret_cast<assign_collations_context*>(context));
			}
			break;
		case T_PGList:
			foreach(temp, (PGList *) node)
			{
				if (walker((PGNode *) lfirst(temp), reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGFromExpr:
			{
				PGFromExpr   *from = (PGFromExpr *) node;

				if (walker(reinterpret_cast<PGNode*>(from->fromlist),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(from->quals, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGJoinExpr:
			{
				PGJoinExpr   *join = (PGJoinExpr *) node;

				if (walker(join->larg, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(join->rarg, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(join->quals, reinterpret_cast<assign_collations_context*>(context)))
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

		// 		if (pg_expression_tree_walker((PGNode *) appinfo->translated_vars,
		// 								   walker, context))
		// 			return true;
		// 	}
		// 	break;
		// case T_PGPlaceHolderInfo:
		// 	return walker(((PGPlaceHolderInfo *) node)->ph_var, context);
		case T_PGRangeTblFunction:
			return walker(((PGRangeTblFunction *) node)->funcexpr, 
                reinterpret_cast<assign_collations_context*>(context));

		// case T_PGGroupingClause:
		// 	{
		// 		PGGroupingClause *g = (PGGroupingClause *) node;
		// 		if (pg_expression_tree_walker((PGNode *)g->groupsets, walker,
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

				if (pg_expression_tree_walker((PGNode *) wd->partitionClause, walker,
										   context))
					return true;
				if (pg_expression_tree_walker((PGNode *) wd->orderClause, walker,
										   context))
					return true;
				if (walker((PGNode *) wd->startOffset, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker((PGNode *) wd->endOffset, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case T_PGTypeCast:
			{
				PGTypeCast *tc = (PGTypeCast *)node;

				if (pg_expression_tree_walker((PGNode*) tc->arg, walker, context))
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

				if (pg_expression_tree_walker((PGNode *) wc->partitionClause, walker,
										   context))
					return true;
				if (pg_expression_tree_walker((PGNode *) wc->orderClause, walker,
										   context))
					return true;
				if (walker((PGNode *) wc->startOffset, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker((PGNode *) wc->endOffset, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				return false;
			}
			break;

		default:
			//elog(ERROR, "unrecognized node type: %d",
				 //(int) nodeTag(node));
			throw DB::Exception(ERROR, "unrecognized node type: {}", (int) nodeTag(node));
	}
	return false;
};

bool
pg_range_table_walker(PGList *rtable,
				   walker_func walker,
				   void *context,
				   int flags)
{
	PGListCell   *rt;

	foreach(rt, rtable)
	{
		PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(rt);

		/* For historical reasons, visiting RTEs is not the default */
		if (flags & QTW_EXAMINE_RTES)
			if (walker((PGNode*)rte, (assign_collations_context*)context))
				return true;

		switch (rte->rtekind)
		{
			case PGRTEKind::PG_RTE_RELATION:
			//case PGRTEKind::PG_RTE_VOID:
			case PGRTEKind::PG_RTE_CTE:
				/* nothing to do */
				break;
			case PGRTEKind::PG_RTE_SUBQUERY:
				if (!(flags & QTW_IGNORE_RT_SUBQUERIES))
					if (walker((PGNode*)rte->subquery, (assign_collations_context*)context))
						return true;
				break;
			case PGRTEKind::PG_RTE_JOIN:
				if (!(flags & QTW_IGNORE_JOINALIASES))
					if (walker((PGNode*)rte->joinaliasvars, (assign_collations_context*)context))
						return true;
				break;
			case PGRTEKind::PG_RTE_FUNCTION:
				if (walker((PGNode*)rte->functions, (assign_collations_context*)context))
					return true;
				break;
			case PGRTEKind::PG_RTE_TABLEFUNC:
				if (walker((PGNode*)rte->subquery, (assign_collations_context*)context))
					return true;
				if (walker((PGNode*)rte->functions, (assign_collations_context*)context))
					return true;
				break;
			case PGRTEKind::PG_RTE_VALUES:
				if (walker((PGNode*)rte->values_lists, (assign_collations_context*)context))
					return true;
				break;
		}

		//if (walker((PGNode*)rte->securityQuals, (assign_collations_context*)context))
			//return true;
	}
	return false;
};

bool
pg_query_tree_walker(PGQuery *query,
				  walker_func walker,
				  void *context,
				  int flags)
{
	Assert(query != NULL && IsA(query, PGQuery));

	/*
	 * We don't walk any utilityStmt here. However, we can't easily assert
	 * that it is absent, since there are at least two code paths by which
	 * action statements from CREATE RULE end up here, and NOTIFY is allowed
	 * in a rule action.
	 */

	if (walker((PGNode *) query->targetList, (assign_collations_context*)context))
		return true;
	if (walker((PGNode *) query->withCheckOptions, (assign_collations_context*)context))
		return true;
	if (walker((PGNode *) query->returningList, (assign_collations_context*)context))
		return true;
	if (walker((PGNode *) query->jointree, (assign_collations_context*)context))
		return true;
	if (walker(query->setOperations, (assign_collations_context*)context))
		return true;
	if (walker(query->havingQual, (assign_collations_context*)context))
		return true;
	if (walker(query->limitOffset, (assign_collations_context*)context))
		return true;
	if (walker(query->limitCount, (assign_collations_context*)context))
		return true;

	/*
	 * Most callers aren't interested in SortGroupClause nodes since those
	 * don't contain actual expressions. However they do contain OIDs which
	 * may be needed by dependency walkers etc.
	 */
	if ((flags & QTW_EXAMINE_SORTGROUP))
	{
		if (walker((PGNode *) query->groupClause, (assign_collations_context*)context))
			return true;
		if (walker((PGNode *) query->windowClause, (assign_collations_context*)context))
			return true;
		if (walker((PGNode *) query->sortClause, (assign_collations_context*)context))
			return true;
		if (walker((PGNode *) query->distinctClause, (assign_collations_context*)context))
			return true;
	}
	else
	{
		/*
		 * But we need to walk the expressions under WindowClause nodes even
		 * if we're not interested in SortGroupClause nodes.
		 */
		PGListCell   *lc;

		foreach(lc, query->windowClause)
		{
			PGWindowClause *wc = lfirst_node(PGWindowClause, lc);

			if (walker(wc->startOffset, (assign_collations_context*)context))
				return true;
			if (walker(wc->endOffset, (assign_collations_context*)context))
				return true;
		}
	}

	/*
	 * groupingSets and rowMarks are not walked:
	 *
	 * groupingSets contain only ressortgrouprefs (integers) which are
	 * meaningless without the corresponding groupClause or tlist.
	 * Accordingly, any walker that needs to care about them needs to handle
	 * them itself in its Query processing.
	 *
	 * rowMarks is not walked because it contains only rangetable indexes (and
	 * flags etc.) and therefore should be handled at Query level similarly.
	 */

	if (!(flags & QTW_IGNORE_CTE_SUBQUERIES))
	{
		if (walker((PGNode *) query->cteList, (assign_collations_context*)context))
			return true;
	}
	if (!(flags & QTW_IGNORE_RANGE_TABLE))
	{
		if (pg_range_table_walker(query->rtable, walker, context, flags))
			return true;
	}
	if (query->utilityStmt)
	{
		/*
		 * Certain utility commands contain general-purpose Querys embedded in
		 * them --- if this is one, invoke the walker on the sub-Query.
		 */
		if (IsA(query->utilityStmt, PGCopyStmt))
		{
			if (walker(((PGCopyStmt *) query->utilityStmt)->query, (assign_collations_context*)context))
				return true;
		}
		// if (IsA(query->utilityStmt, DeclareCursorStmt))
		// {
		// 	if (walker(((DeclareCursorStmt *) query->utilityStmt)->query, context))
		// 		return true;
		// }
		if (IsA(query->utilityStmt, PGExplainStmt))
		{
			if (walker(((PGExplainStmt *) query->utilityStmt)->query, (assign_collations_context*)context))
				return true;
		}
		if (IsA(query->utilityStmt, PGPrepareStmt))
		{
			if (walker(((PGPrepareStmt *) query->utilityStmt)->query, (assign_collations_context*)context))
				return true;
		}
		if (IsA(query->utilityStmt, PGViewStmt))
		{
			if (walker(((PGViewStmt *) query->utilityStmt)->query, (assign_collations_context*)context))
				return true;
		}
	}
	return false;
};

bool
pg_contain_aggs_of_level_walker(PGNode *node,
							 contain_aggs_of_level_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGAggref))
	{
		if (((PGAggref *) node)->agglevelsup == context->sublevels_up)
			return true;		/* abort the tree traversal and return true */
		/* else fall through to examine argument */
	}
	if (IsA(node, PGQuery))
	{
		/* Recurse into subselects */
		bool		result;

		context->sublevels_up++;
		result = pg_query_tree_walker((PGQuery *) node,
								   contain_aggs_of_level_walker,
								   context, 0);
		context->sublevels_up--;
		return result;
	}
	return pg_expression_tree_walker(node, contain_aggs_of_level_walker,
								  context);
};

bool
pg_contain_aggs_of_level(PGNode *node, int levelsup)
{
	contain_aggs_of_level_context context;

	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	return query_or_expression_tree_walker(node,
										   contain_aggs_of_level_walker,
										   &context,
										   0);
};

bool
pg_contain_windowfuncs_walker(PGNode *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGWindowFunc))
		return true;			/* abort the tree traversal and return true */
	/* Mustn't recurse into subselects */
	return pg_expression_tree_walker(node, contain_windowfuncs_walker,
								  context);
};

bool
pg_contain_windowfuncs(PGNode *node)
{
	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	return pg_query_or_expression_tree_walker(node,
										   contain_windowfuncs_walker,
										   NULL,
										   0);
};

bool
pg_query_or_expression_tree_walker(PGNode *node,
								walker_func walker,
								void *context,
								int flags)
{
	if (node && IsA(node, PGQuery))
		return pg_query_tree_walker((PGQuery *) node,
								 walker,
								 context,
								 flags);
	else
		return walker(node, (assign_collations_context*)context);
};


bool
pg_locate_agg_of_level_walker(PGNode *node,
		locate_agg_of_level_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGAggref))
	{
		if (((PGAggref *) node)->agglevelsup == context->sublevels_up &&
			((PGAggref *) node)->location >= 0)
		{
			context->agg_location = ((PGAggref *) node)->location;
			return true;		/* abort the tree traversal and return true */
		}
		/* else fall through to examine argument */
	}
	if (IsA(node, PGGroupingFunc))
	{
		if (((PGGroupingFunc *) node)->agglevelsup == context->sublevels_up &&
			((PGGroupingFunc *) node)->location >= 0)
		{
			context->agg_location = ((PGGroupingFunc *) node)->location;
			return true;		/* abort the tree traversal and return true */
		}
	}
	if (IsA(node, PGQuery))
	{
		/* Recurse into subselects */
		bool		result;

		context->sublevels_up++;
		result = pg_query_tree_walker((PGQuery *) node,
								   (walker_func)pg_locate_agg_of_level_walker,
								   (void *) context, 0);
		context->sublevels_up--;
		return result;
	}
	return pg_expression_tree_walker(node, (walker_func)pg_locate_agg_of_level_walker,
								  (void *) context);
};

int
pg_locate_agg_of_level(PGNode *node, int levelsup)
{
	locate_agg_of_level_context context;

	context.agg_location = -1;	/* in case we find nothing */
	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	(void) pg_query_or_expression_tree_walker(node,
										   (walker_func)pg_locate_agg_of_level_walker,
										   (void *) &context,
										   0);

	return context.agg_location;
};

int
pg_locate_var_of_level(PGNode *node, int levelsup)
{
	locate_var_of_level_context context;

	context.var_location = -1;	/* in case we find nothing */
	context.sublevels_up = levelsup;

	(void) pg_query_or_expression_tree_walker(node,
										   (walker_func)pg_locate_var_of_level_walker,
										   (void *) &context,
										   0);

	return context.var_location;
};

bool
pg_locate_var_of_level_walker(PGNode *node,
						   locate_var_of_level_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGVar))
	{
		PGVar		   *var = (PGVar *) node;

		if (var->varlevelsup == context->sublevels_up &&
			var->location >= 0)
		{
			context->var_location = var->location;
			return true;		/* abort tree traversal and return true */
		}
		return false;
	}
	if (IsA(node, PGCurrentOfExpr))
	{
		/* since CurrentOfExpr doesn't carry location, nothing we can do */
		return false;
	}
	/* No extra code needed for PlaceHolderVar; just look in contained expr */
	if (IsA(node, PGQuery))
	{
		/* Recurse into subselects */
		bool		result;

		context->sublevels_up++;
		result = pg_query_tree_walker((PGQuery *) node,
								   (walker_func)pg_locate_var_of_level_walker,
								   (void *) context,
								   0);
		context->sublevels_up--;
		return result;
	}
	return pg_expression_tree_walker(node,
								  (walker_func)pg_locate_var_of_level_walker,
								  (void *) context);
};
