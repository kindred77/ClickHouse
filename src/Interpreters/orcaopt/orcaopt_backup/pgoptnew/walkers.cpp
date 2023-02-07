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
pg_contain_windowfuncs_walker(PGNode *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGWindowFunc))
		return true;			/* abort the tree traversal and return true */
	/* Mustn't recurse into subselects */
	return pg_expression_tree_walker(node, (walker_func)pg_contain_windowfuncs_walker,
								  (void *) context);
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
								   (walker_func)pg_contain_aggs_of_level_walker,
								   context, 0);
		context->sublevels_up--;
		return result;
	}
	return pg_expression_tree_walker(node, (walker_func)pg_contain_aggs_of_level_walker,
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
	return pg_query_or_expression_tree_walker(node,
										   (walker_func)pg_contain_aggs_of_level_walker,
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
	return pg_expression_tree_walker(node, (walker_func)pg_contain_windowfuncs_walker,
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
										   (walker_func)pg_contain_windowfuncs_walker,
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

bool
pg_contain_windowfuncs_walker(PGNode *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGWindowFunc))
		return true;			/* abort the tree traversal and return true */
	/* Mustn't recurse into subselects */
	return pg_expression_tree_walker(node, (walker_func)pg_contain_windowfuncs_walker,
								  (void *) context);
};

bool
pg_contain_windowfuncs(PGNode *node)
{
	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	return pg_query_or_expression_tree_walker(node,
										   (walker_func)pg_contain_windowfuncs_walker,
										   NULL,
										   0);
};

bool
pg_locate_windowfunc_walker(PGNode *node, locate_windowfunc_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGWindowFunc))
	{
		if (((PGWindowFunc *) node)->location >= 0)
		{
			context->win_location = ((PGWindowFunc *) node)->location;
			return true;		/* abort the tree traversal and return true */
		}
		/* else fall through to examine argument */
	}
	/* Mustn't recurse into subselects */
	return pg_expression_tree_walker(node, (walker_func)pg_locate_windowfunc_walker,
								  (void *) context);
};

int
pg_locate_windowfunc(PGNode *node)
{
	locate_windowfunc_context context;

	context.win_location = -1;	/* in case we find nothing */

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	(void) pg_query_or_expression_tree_walker(node,
										   (walker_func)pg_locate_windowfunc_walker,
										   (void *) &context,
										   0);

	return context.win_location;
};

bool
PGIncrementVarSublevelsUp_walker(PGNode *node,
							   IncrementVarSublevelsUp_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGVar))
	{
		PGVar		   *var = (PGVar *) node;

		if (var->varlevelsup >= context->min_sublevels_up)
			var->varlevelsup += context->delta_sublevels_up;
		return false;			/* done here */
	}
	if (IsA(node, PGCurrentOfExpr))
	{
		/* this should not happen */
		if (context->min_sublevels_up == 0)
			elog(ERROR, "cannot push down CurrentOfExpr");
		return false;
	}
	if (IsA(node, PGAggref))
	{
		PGAggref	   *agg = (PGAggref *) node;

		if (agg->agglevelsup >= context->min_sublevels_up)
			agg->agglevelsup += context->delta_sublevels_up;
		/* fall through to recurse into argument */
	}
	if (IsA(node, PGGroupingFunc))
	{
		PGGroupingFunc *grp = (PGGroupingFunc *) node;

		if (grp->agglevelsup >= context->min_sublevels_up)
			grp->agglevelsup += context->delta_sublevels_up;
		/* fall through to recurse into argument */
	}
	// if (IsA(node, PlaceHolderVar))
	// {
	// 	PlaceHolderVar *phv = (PlaceHolderVar *) node;

	// 	if (phv->phlevelsup >= context->min_sublevels_up)
	// 		phv->phlevelsup += context->delta_sublevels_up;
	// 	/* fall through to recurse into argument */
	// }
	if (IsA(node, PGRangeTblEntry))
	{
		PGRangeTblEntry *rte = (PGRangeTblEntry *) node;

		if (rte->rtekind == PG_RTE_CTE)
		{
			if (rte->ctelevelsup >= context->min_sublevels_up)
				rte->ctelevelsup += context->delta_sublevels_up;

			/*
			* Fix for MPP-19436: in transformGroupedWindows, min_sublevels_up
			* is ignored. For RTE refer to the original query ctelist should
			* all be incremented.
			*/
			if(context->ignore_min_sublevels_up && rte->ctelevelsup == context->min_sublevels_up - 1)
			{
				rte->ctelevelsup += context->delta_sublevels_up;
			}
		}
		return false;			/* allow range_table_walker to continue */
	}
	if (IsA(node, PGQuery))
	{
		/* Recurse into subselects */
		bool		result;

		context->min_sublevels_up++;
		result = pg_query_tree_walker((PGQuery *) node,
								   (walker_func)PGIncrementVarSublevelsUp_walker,
								   (void *) context,
								   QTW_EXAMINE_RTES_BEFORE);
		context->min_sublevels_up--;
		return result;
	}
	return pg_expression_tree_walker(node, (walker_func)PGIncrementVarSublevelsUp_walker,
								  (void *) context);
};

void
PGIncrementVarSublevelsUp(PGNode *node, int delta_sublevels_up,
						int min_sublevels_up)
{
	IncrementVarSublevelsUp_context context;

	context.delta_sublevels_up = delta_sublevels_up;
	context.min_sublevels_up = min_sublevels_up;
	context.ignore_min_sublevels_up = false;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	pg_query_or_expression_tree_walker(node,
									(walker_func)PGIncrementVarSublevelsUp_walker,
									(void *) &context,
									QTW_EXAMINE_RTES_BEFORE);
};

extern PGNode *
pg_expression_tree_mutator(PGNode *node,
						PGNode *(*mutator) (PGNode *node, void *context),
						void *context)
{
	/*
	 * The mutator has already decided not to modify the current node, but we
	 * must call the mutator for any sub-nodes.
	 */

#define FLATCOPY(newnode, node, nodetype)  \
	( (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define CHECKFLATCOPY(newnode, node, nodetype)	\
	( AssertMacro(IsA((node), nodetype)), \
	  (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define MUTATE(newfield, oldfield, fieldtype)  \
		( (newfield) = (fieldtype) mutator((PGNode *) (oldfield), context) )

	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
			/*
			 * Primitive node types with no expression subnodes.  Var and
			 * Const are frequent enough to deserve special cases, the others
			 * we just use copyObject for.
			 */
		case T_PGVar:
			{
				PGVar		   *var = (PGVar *) node;
				PGVar		   *newnode;

				FLATCOPY(newnode, var, PGVar);
				return (PGNode *) newnode;
			}
			break;
		case T_PGConst:
			{
				PGConst	   *oldnode = (PGConst *) node;
				PGConst	   *newnode;

				FLATCOPY(newnode, oldnode, PGConst);
				/* XXX we don't bother with datumCopy; should we? */
				return (PGNode *) newnode;
			}
			break;
		case T_PGParam:
		case T_PGCaseTestExpr:
		case T_PGSQLValueFunction:
		case T_PGCoerceToDomainValue:
		case T_PGSetToDefault:
		case T_PGCurrentOfExpr:
		case T_PGNextValueExpr:
		case T_PGRangeTblRef:
		case T_PGString:
		case T_PGNull:
			return (PGNode *) copyObject(node);
		// case T_PGWithCheckOption:
		// 	{
		// 		WithCheckOption *wco = (WithCheckOption *) node;
		// 		WithCheckOption *newnode;

		// 		FLATCOPY(newnode, wco, WithCheckOption);
		// 		MUTATE(newnode->qual, wco->qual, Node *);
		// 		return (Node *) newnode;
		// 	}
		case T_PGAggref:
			{
				PGAggref	   *aggref = (PGAggref *) node;
				PGAggref	   *newnode;

				FLATCOPY(newnode, aggref, PGAggref);
				/* assume mutation doesn't change types of arguments */
				newnode->aggargtypes = list_copy(aggref->aggargtypes);
				MUTATE(newnode->aggdirectargs, aggref->aggdirectargs, PGList *);
				MUTATE(newnode->args, aggref->args, PGList *);
				MUTATE(newnode->aggorder, aggref->aggorder, PGList *);
				MUTATE(newnode->aggdistinct, aggref->aggdistinct, PGList *);
				MUTATE(newnode->aggfilter, aggref->aggfilter, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		// case T_DQAExpr:
		// 	{
		// 		DQAExpr *dqaExpr = (DQAExpr *)node;
		// 		DQAExpr *newDqaExpr;
		// 		FLATCOPY(newDqaExpr, dqaExpr, DQAExpr);
		// 		MUTATE(newDqaExpr->agg_filter, dqaExpr->agg_filter, Expr *);
		// 		return (Node *)newDqaExpr;
		// 	}
        //     break;
		case T_PGGroupingFunc:
			{
				PGGroupingFunc   *grouping = (PGGroupingFunc *) node;
				PGGroupingFunc   *newnode;

				FLATCOPY(newnode, grouping, PGGroupingFunc);
				MUTATE(newnode->args, grouping->args, PGList *);

				/*
				 * We assume here that mutating the arguments does not change
				 * the semantics, i.e. that the arguments are not mutated in a
				 * way that makes them semantically different from their
				 * previously matching expressions in the GROUP BY clause.
				 *
				 * If a mutator somehow wanted to do this, it would have to
				 * handle the refs and cols lists itself as appropriate.
				 */
				newnode->refs = list_copy(grouping->refs);
				newnode->cols = list_copy(grouping->cols);

				return (PGNode *) newnode;
			}
			break;
		// case T_GroupId:
		// 	{
		// 		GroupId   *groupid = (GroupId *) node;
		// 		GroupId   *newnode;

		// 		FLATCOPY(newnode, groupid, GroupId);

		// 		return (Node *) newnode;
		// 	}
		// 	break;
		// case T_GroupingSetId:
		// 	{
		// 		GroupingSetId   *gsetid = (GroupingSetId *) node;
		// 		GroupingSetId   *newnode;

		// 		FLATCOPY(newnode, gsetid, GroupingSetId);

		// 		return (Node *) newnode;
		// 	}
		// 	break;
		case T_PGWindowFunc:
			{
				PGWindowFunc *wfunc = (PGWindowFunc *) node;
				PGWindowFunc *newnode;

				FLATCOPY(newnode, wfunc, PGWindowFunc);
				MUTATE(newnode->args, wfunc->args, PGList *);
				MUTATE(newnode->aggfilter, wfunc->aggfilter, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		// case T_SubscriptingRef:
		// 	{
		// 		SubscriptingRef *sbsref = (SubscriptingRef *) node;
		// 		SubscriptingRef *newnode;

		// 		FLATCOPY(newnode, sbsref, SubscriptingRef);
		// 		MUTATE(newnode->refupperindexpr, sbsref->refupperindexpr,
		// 			   List *);
		// 		MUTATE(newnode->reflowerindexpr, sbsref->reflowerindexpr,
		// 			   List *);
		// 		MUTATE(newnode->refexpr, sbsref->refexpr,
		// 			   Expr *);
		// 		MUTATE(newnode->refassgnexpr, sbsref->refassgnexpr,
		// 			   Expr *);

		// 		return (Node *) newnode;
		// 	}
		// 	break;
		case T_PGFuncExpr:
			{
				PGFuncExpr   *expr = (PGFuncExpr *) node;
				PGFuncExpr   *newnode;

				FLATCOPY(newnode, expr, PGFuncExpr);
				MUTATE(newnode->args, expr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		// case T_TableValueExpr:
		// 	{
		// 		TableValueExpr   *expr = (TableValueExpr *) node;
		// 		TableValueExpr   *newnode;

		// 		FLATCOPY(newnode, expr, TableValueExpr);

		// 		/* The subquery already pulled up into the T_TableFunctionScan node */
		// 		newnode->subquery = (Node *) NULL;
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		case T_PGNamedArgExpr:
			{
				PGNamedArgExpr *nexpr = (PGNamedArgExpr *) node;
				PGNamedArgExpr *newnode;

				FLATCOPY(newnode, nexpr, PGNamedArgExpr);
				MUTATE(newnode->arg, nexpr->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGOpExpr:
			{
				PGOpExpr	   *expr = (PGOpExpr *) node;
				PGOpExpr	   *newnode;

				FLATCOPY(newnode, expr, PGOpExpr);
				MUTATE(newnode->args, expr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGDistinctExpr:
			{
				DistinctExpr *expr = (DistinctExpr *) node;
				DistinctExpr *newnode;

				FLATCOPY(newnode, expr, DistinctExpr);
				MUTATE(newnode->args, expr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGNullIfExpr:
			{
				NullIfExpr *expr = (NullIfExpr *) node;
				NullIfExpr *newnode;

				FLATCOPY(newnode, expr, NullIfExpr);
				MUTATE(newnode->args, expr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGScalarArrayOpExpr:
			{
				PGScalarArrayOpExpr *expr = (PGScalarArrayOpExpr *) node;
				PGScalarArrayOpExpr *newnode;

				FLATCOPY(newnode, expr, PGScalarArrayOpExpr);
				MUTATE(newnode->args, expr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGBoolExpr:
			{
				PGBoolExpr   *expr = (PGBoolExpr *) node;
				PGBoolExpr   *newnode;

				FLATCOPY(newnode, expr, PGBoolExpr);
				MUTATE(newnode->args, expr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGSubLink:
			{
				PGSubLink    *sublink = (PGSubLink *) node;
				PGSubLink    *newnode;

				FLATCOPY(newnode, sublink, PGSubLink);
				MUTATE(newnode->testexpr, sublink->testexpr, PGNode *);

				/*
				 * Also invoke the mutator on the sublink's Query node, so it
				 * can recurse into the sub-query if it wants to.
				 */
				MUTATE(newnode->subselect, sublink->subselect, PGNode *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGSubPlan:
			{
				PGSubPlan    *subplan = (PGSubPlan *) node;
				PGSubPlan    *newnode;

				FLATCOPY(newnode, subplan, PGSubPlan);
				/* transform testexpr */
				MUTATE(newnode->testexpr, subplan->testexpr, PGNode *);
				/* transform args list (params to be passed to subplan) */
				MUTATE(newnode->args, subplan->args, PGList *);
				/* but not the sub-Plan itself, which is referenced as-is */
				return (PGNode *) newnode;
			}
			break;
		case T_PGAlternativeSubPlan:
			{
				PGAlternativeSubPlan *asplan = (PGAlternativeSubPlan *) node;
				PGAlternativeSubPlan *newnode;

				FLATCOPY(newnode, asplan, PGAlternativeSubPlan);
				MUTATE(newnode->subplans, asplan->subplans, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGFieldSelect:
			{
				PGFieldSelect *fselect = (PGFieldSelect *) node;
				PGFieldSelect *newnode;

				FLATCOPY(newnode, fselect, PGFieldSelect);
				MUTATE(newnode->arg, fselect->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGFieldStore:
			{
				PGFieldStore *fstore = (PGFieldStore *) node;
				PGFieldStore *newnode;

				FLATCOPY(newnode, fstore, PGFieldStore);
				MUTATE(newnode->arg, fstore->arg, PGExpr *);
				MUTATE(newnode->newvals, fstore->newvals, PGList *);
				newnode->fieldnums = list_copy(fstore->fieldnums);
				return (PGNode *) newnode;
			}
			break;
		case T_PGRelabelType:
			{
				PGRelabelType *relabel = (PGRelabelType *) node;
				PGRelabelType *newnode;

				FLATCOPY(newnode, relabel, PGRelabelType);
				MUTATE(newnode->arg, relabel->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGCoerceViaIO:
			{
				PGCoerceViaIO *iocoerce = (PGCoerceViaIO *) node;
				PGCoerceViaIO *newnode;

				FLATCOPY(newnode, iocoerce, PGCoerceViaIO);
				MUTATE(newnode->arg, iocoerce->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGArrayCoerceExpr:
			{
				PGArrayCoerceExpr *acoerce = (PGArrayCoerceExpr *) node;
				PGArrayCoerceExpr *newnode;

				FLATCOPY(newnode, acoerce, PGArrayCoerceExpr);
				MUTATE(newnode->arg, acoerce->arg, PGExpr *);
				MUTATE(newnode->elemexpr, acoerce->elemexpr, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGConvertRowtypeExpr:
			{
				PGConvertRowtypeExpr *convexpr = (PGConvertRowtypeExpr *) node;
				PGConvertRowtypeExpr *newnode;

				FLATCOPY(newnode, convexpr, PGConvertRowtypeExpr);
				MUTATE(newnode->arg, convexpr->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGCollateExpr:
			{
				PGCollateExpr *collate = (PGCollateExpr *) node;
				PGCollateExpr *newnode;

				FLATCOPY(newnode, collate, PGCollateExpr);
				MUTATE(newnode->arg, collate->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGCaseExpr:
			{
				PGCaseExpr   *caseexpr = (PGCaseExpr *) node;
				PGCaseExpr   *newnode;

				FLATCOPY(newnode, caseexpr, PGCaseExpr);
				MUTATE(newnode->arg, caseexpr->arg, PGExpr *);
				MUTATE(newnode->args, caseexpr->args, PGList *);
				MUTATE(newnode->defresult, caseexpr->defresult, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGCaseWhen:
			{
				PGCaseWhen   *casewhen = (PGCaseWhen *) node;
				PGCaseWhen   *newnode;

				FLATCOPY(newnode, casewhen, PGCaseWhen);
				MUTATE(newnode->expr, casewhen->expr, PGExpr *);
				MUTATE(newnode->result, casewhen->result, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGArrayExpr:
			{
				PGArrayExpr  *arrayexpr = (PGArrayExpr *) node;
				PGArrayExpr  *newnode;

				FLATCOPY(newnode, arrayexpr, PGArrayExpr);
				MUTATE(newnode->elements, arrayexpr->elements, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGRowExpr:
			{
				PGRowExpr    *rowexpr = (PGRowExpr *) node;
				PGRowExpr    *newnode;

				FLATCOPY(newnode, rowexpr, PGRowExpr);
				MUTATE(newnode->args, rowexpr->args, PGList *);
				/* Assume colnames needn't be duplicated */
				return (PGNode *) newnode;
			}
			break;
		case T_PGRowCompareExpr:
			{
				PGRowCompareExpr *rcexpr = (PGRowCompareExpr *) node;
				PGRowCompareExpr *newnode;

				FLATCOPY(newnode, rcexpr, PGRowCompareExpr);
				MUTATE(newnode->largs, rcexpr->largs, PGList *);
				MUTATE(newnode->rargs, rcexpr->rargs, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGCoalesceExpr:
			{
				PGCoalesceExpr *coalesceexpr = (PGCoalesceExpr *) node;
				PGCoalesceExpr *newnode;

				FLATCOPY(newnode, coalesceexpr, PGCoalesceExpr);
				MUTATE(newnode->args, coalesceexpr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGMinMaxExpr:
			{
				PGMinMaxExpr *minmaxexpr = (PGMinMaxExpr *) node;
				PGMinMaxExpr *newnode;

				FLATCOPY(newnode, minmaxexpr, PGMinMaxExpr);
				MUTATE(newnode->args, minmaxexpr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		// case T_XmlExpr:
		// 	{
		// 		XmlExpr    *xexpr = (XmlExpr *) node;
		// 		XmlExpr    *newnode;

		// 		FLATCOPY(newnode, xexpr, XmlExpr);
		// 		MUTATE(newnode->named_args, xexpr->named_args, List *);
		// 		/* assume mutator does not care about arg_names */
		// 		MUTATE(newnode->args, xexpr->args, List *);
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		case T_PGNullTest:
			{
				PGNullTest   *ntest = (PGNullTest *) node;
				PGNullTest   *newnode;

				FLATCOPY(newnode, ntest, PGNullTest);
				MUTATE(newnode->arg, ntest->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGBooleanTest:
			{
				PGBooleanTest *btest = (PGBooleanTest *) node;
				PGBooleanTest *newnode;

				FLATCOPY(newnode, btest, PGBooleanTest);
				MUTATE(newnode->arg, btest->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGCoerceToDomain:
			{
				PGCoerceToDomain *ctest = (PGCoerceToDomain *) node;
				PGCoerceToDomain *newnode;

				FLATCOPY(newnode, ctest, PGCoerceToDomain);
				MUTATE(newnode->arg, ctest->arg, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGTargetEntry:
			{
				PGTargetEntry *targetentry = (PGTargetEntry *) node;
				PGTargetEntry *newnode;

				FLATCOPY(newnode, targetentry, PGTargetEntry);
				MUTATE(newnode->expr, targetentry->expr, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGQuery:
			/* Do nothing with a sub-Query, per discussion above */
			return node;
		case T_PGWindowClause:
			{
				PGWindowClause *wc = (PGWindowClause *) node;
				PGWindowClause *newnode;

				FLATCOPY(newnode, wc, PGWindowClause);

				MUTATE(newnode->partitionClause, wc->partitionClause, PGList *);
				MUTATE(newnode->orderClause, wc->orderClause, PGList *);
				MUTATE(newnode->startOffset, wc->startOffset, PGNode *);
				MUTATE(newnode->endOffset, wc->endOffset, PGNode *);
				return (PGNode *) newnode;

			}
		case T_PGCommonTableExpr:
			{
				PGCommonTableExpr *cte = (PGCommonTableExpr *) node;
				PGCommonTableExpr *newnode;

				FLATCOPY(newnode, cte, PGCommonTableExpr);

				/*
				 * Also invoke the mutator on the CTE's Query node, so it can
				 * recurse into the sub-query if it wants to.
				 */
				MUTATE(newnode->ctequery, cte->ctequery, PGNode *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGList:
			{
				/*
				 * We assume the mutator isn't interested in the list nodes
				 * per se, so just invoke it on each list element. NOTE: this
				 * would fail badly on a list with integer elements!
				 */
				PGList	   *resultlist;
				PGListCell   *temp;

				resultlist = NIL;
				foreach(temp, (PGList *) node)
				{
					resultlist = lappend(resultlist,
										 mutator((PGNode *) lfirst(temp),
												 context));
				}
				return (PGNode *) resultlist;
			}
			break;
		case T_PGFromExpr:
			{
				PGFromExpr   *from = (PGFromExpr *) node;
				PGFromExpr   *newnode;

				FLATCOPY(newnode, from, PGFromExpr);
				MUTATE(newnode->fromlist, from->fromlist, PGList *);
				MUTATE(newnode->quals, from->quals, PGNode *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGOnConflictExpr:
			{
				PGOnConflictExpr *oc = (PGOnConflictExpr *) node;
				PGOnConflictExpr *newnode;

				FLATCOPY(newnode, oc, PGOnConflictExpr);
				MUTATE(newnode->arbiterElems, oc->arbiterElems, PGList *);
				MUTATE(newnode->arbiterWhere, oc->arbiterWhere, PGNode *);
				MUTATE(newnode->onConflictSet, oc->onConflictSet, PGList *);
				MUTATE(newnode->onConflictWhere, oc->onConflictWhere, PGNode *);
				MUTATE(newnode->exclRelTlist, oc->exclRelTlist, PGList *);

				return (PGNode *) newnode;
			}
			break;
		// case T_PartitionPruneStepOp:
		// 	{
		// 		PartitionPruneStepOp *opstep = (PartitionPruneStepOp *) node;
		// 		PartitionPruneStepOp *newnode;

		// 		FLATCOPY(newnode, opstep, PartitionPruneStepOp);
		// 		MUTATE(newnode->exprs, opstep->exprs, List *);

		// 		return (Node *) newnode;
		// 	}
		// 	break;
		// case T_PartitionPruneStepCombine:
		// 	/* no expression sub-nodes */
		// 	return (Node *) copyObject(node);
		case T_PGJoinExpr:
			{
				PGJoinExpr   *join = (PGJoinExpr *) node;
				PGJoinExpr   *newnode;

				FLATCOPY(newnode, join, PGJoinExpr);
				MUTATE(newnode->larg, join->larg, PGNode *);
				MUTATE(newnode->rarg, join->rarg, PGNode *);
				MUTATE(newnode->quals, join->quals, PGNode *);
				/* We do not mutate alias or using by default */
				return (PGNode *) newnode;
			}
			break;
		// case T_SetOperationStmt:
		// 	{
		// 		SetOperationStmt *setop = (SetOperationStmt *) node;
		// 		SetOperationStmt *newnode;

		// 		FLATCOPY(newnode, setop, SetOperationStmt);
		// 		MUTATE(newnode->larg, setop->larg, Node *);
		// 		MUTATE(newnode->rarg, setop->rarg, Node *);
		// 		/* We do not mutate groupClauses by default */
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		// case T_IndexClause:
		// 	{
		// 		IndexClause *iclause = (IndexClause *) node;
		// 		IndexClause *newnode;

		// 		FLATCOPY(newnode, iclause, IndexClause);
		// 		MUTATE(newnode->rinfo, iclause->rinfo, RestrictInfo *);
		// 		MUTATE(newnode->indexquals, iclause->indexquals, List *);
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		// case T_PlaceHolderVar:
		// 	{
		// 		PlaceHolderVar *phv = (PlaceHolderVar *) node;
		// 		PlaceHolderVar *newnode;

		// 		FLATCOPY(newnode, phv, PlaceHolderVar);
		// 		MUTATE(newnode->phexpr, phv->phexpr, Expr *);
		// 		/* Assume we need not copy the relids bitmapset */
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		case T_PGInferenceElem:
			{
				PGInferenceElem *inferenceelemdexpr = (PGInferenceElem *) node;
				PGInferenceElem *newnode;

				FLATCOPY(newnode, inferenceelemdexpr, PGInferenceElem);
				MUTATE(newnode->expr, newnode->expr, PGNode *);
				return (PGNode *) newnode;
			}
			break;
		// case T_AppendRelInfo:
		// 	{
		// 		AppendRelInfo *appinfo = (AppendRelInfo *) node;
		// 		AppendRelInfo *newnode;

		// 		FLATCOPY(newnode, appinfo, AppendRelInfo);
		// 		MUTATE(newnode->translated_vars, appinfo->translated_vars, List *);
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		// case T_PlaceHolderInfo:
		// 	{
		// 		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) node;
		// 		PlaceHolderInfo *newnode;

		// 		FLATCOPY(newnode, phinfo, PlaceHolderInfo);
		// 		MUTATE(newnode->ph_var, phinfo->ph_var, PlaceHolderVar *);
		// 		/* Assume we need not copy the relids bitmapsets */
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		case T_PGRangeTblFunction:
			{
				PGRangeTblFunction *rtfunc = (PGRangeTblFunction *) node;
				PGRangeTblFunction *newnode;

				FLATCOPY(newnode, rtfunc, PGRangeTblFunction);
				MUTATE(newnode->funcexpr, rtfunc->funcexpr, PGNode *);
				/* Assume we need not copy the coldef info lists */
				return (PGNode *) newnode;
			}
			break;
		// case T_TableFunctionScan:
		// 	{
		// 		TableFunctionScan *tablefunc = (TableFunctionScan *) node;
		// 		TableFunctionScan *newnode;

		// 		FLATCOPY(newnode, tablefunc, TableFunctionScan);
		// 		return (Node *) newnode;
		// 	}
		// 	break;
		case T_PGWindowDef:
			{
				PGWindowDef *windef = (PGWindowDef *) node;
				PGWindowDef *newnode;

				FLATCOPY(newnode, windef, PGWindowDef);

				MUTATE(newnode->partitionClause, windef->partitionClause, PGList *);
				MUTATE(newnode->orderClause, windef->orderClause, PGList *);
				MUTATE(newnode->startOffset, windef->startOffset, PGNode *);
				MUTATE(newnode->endOffset, windef->endOffset, PGNode *);

				return (PGNode *) newnode;

			}
			break;
		case T_PGSortGroupClause:
			{
				PGSortGroupClause *sortcl = (PGSortGroupClause *) node;
				PGSortGroupClause *newnode;

				FLATCOPY(newnode, sortcl, PGSortGroupClause);

				return (PGNode *) newnode;
			}
			break;
		// case T_DMLActionExpr:
		// 	{
		// 		DMLActionExpr *action_expr = (DMLActionExpr *) node;
		// 		DMLActionExpr *new_action_expr;

		// 		FLATCOPY(new_action_expr, action_expr, DMLActionExpr);
		// 		return (Node *)new_action_expr;
		// 	}
		// 	break;
		case T_PGTableSampleClause:
			{
				PGTableSampleClause *tsc = (PGTableSampleClause *) node;
				PGTableSampleClause *newnode;

				FLATCOPY(newnode, tsc, PGTableSampleClause);
				MUTATE(newnode->args, tsc->args, PGList *);
				MUTATE(newnode->repeatable, tsc->repeatable, PGExpr *);
				return (PGNode *) newnode;
			}
			break;
		// case T_AggExprId:
		// 	{
		// 		AggExprId *exprId = (AggExprId *)node;
		// 		AggExprId *new_exprId;
		// 		FLATCOPY(new_exprId, exprId, AggExprId);
		// 		return (Node *)new_exprId;
		// 	}
		// 	break;
		// case T_RowIdExpr:
		// 	{
		// 		RowIdExpr *rowidexpr = (RowIdExpr *) node;
		// 		RowIdExpr *newnode;

		// 		FLATCOPY(newnode, rowidexpr, RowIdExpr);
		// 		return (Node *) newnode;
		// 	}
		// 	break;

		case T_PGTableFunc:
			{
				PGTableFunc  *tf = (PGTableFunc *) node;
				PGTableFunc  *newnode;

				FLATCOPY(newnode, tf, PGTableFunc);
				MUTATE(newnode->ns_uris, tf->ns_uris, PGList *);
				MUTATE(newnode->docexpr, tf->docexpr, PGNode *);
				MUTATE(newnode->rowexpr, tf->rowexpr, PGNode *);
				MUTATE(newnode->colexprs, tf->colexprs, PGList *);
				MUTATE(newnode->coldefexprs, tf->coldefexprs, PGList *);
				return (PGNode *) newnode;
			}
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
	/* can't get here, but keep compiler happy */
	return NULL;
};

PGList *
pg_range_table_mutator(PGList *rtable,
					PGNode *(*mutator) (PGNode *node, void *context),
					void *context,
					int flags)
{
	PGList	   *newrt = NIL;
	PGListCell   *rt;

	foreach(rt, rtable)
	{
		PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(rt);
		PGRangeTblEntry *newrte;

		FLATCOPY(newrte, rte, PGRangeTblEntry);
		switch (rte->rtekind)
		{
			case PG_RTE_RELATION:
				MUTATE(newrte->tablesample, rte->tablesample,
					   PGTableSampleClause *);
				/* we don't bother to copy eref, aliases, etc; OK? */
				break;
			case PG_RTE_SUBQUERY:
				if (!(flags & QTW_IGNORE_RT_SUBQUERIES))
				{
					CHECKFLATCOPY(newrte->subquery, rte->subquery, PGQuery);
					MUTATE(newrte->subquery, newrte->subquery, PGQuery *);
				}
				else
				{
					/* else, copy RT subqueries as-is */
					newrte->subquery = (PGQuery *)copyObject(rte->subquery);
				}
				break;
			case PG_RTE_JOIN:
				if (!(flags & QTW_IGNORE_JOINALIASES))
					MUTATE(newrte->joinaliasvars, rte->joinaliasvars, PGList *);
				else
				{
					/* else, copy join aliases as-is */
					newrte->joinaliasvars = (PGList *)copyObject(rte->joinaliasvars);
				}
				break;
			case PG_RTE_FUNCTION:
				MUTATE(newrte->functions, rte->functions, PGList *);
				break;
			// case RTE_TABLEFUNCTION:
			// 	MUTATE(newrte->functions, rte->functions, PGList *);
			// 	MUTATE(newrte->subquery, rte->subquery, PGQuery *);
			// 	break;
			case PG_RTE_TABLEFUNC:
				MUTATE(newrte->tablefunc, rte->tablefunc, PGTableFunc *);
				break;
			case PG_RTE_VALUES:
				MUTATE(newrte->values_lists, rte->values_lists, PGList *);
				break;
			case PG_RTE_CTE:
			case RTE_NAMEDTUPLESTORE:
			// case RTE_RESULT:
			// case RTE_VOID:
			// 	/* nothing to do */
				break;
		}
		MUTATE(newrte->securityQuals, rte->securityQuals, PGList *);
		newrt = lappend(newrt, newrte);
	}
	return newrt;
};

PGQuery *
pg_query_tree_mutator(PGQuery *query,
				   PGNode *(*mutator) (PGNode *node, void *context),
				   void *context,
				   int flags)
{
	Assert(query != NULL && IsA(query, PGQuery));

	if (!(flags & QTW_DONT_COPY_QUERY))
	{
		PGQuery	   *newquery;

		FLATCOPY(newquery, query, PGQuery);
		query = newquery;
	}

	MUTATE(query->targetList, query->targetList, PGList *);
	MUTATE(query->withCheckOptions, query->withCheckOptions, PGList *);
	MUTATE(query->onConflict, query->onConflict, PGOnConflictExpr *);
	MUTATE(query->returningList, query->returningList, PGList *);
	MUTATE(query->jointree, query->jointree, PGFromExpr *);
	MUTATE(query->setOperations, query->setOperations, PGNode *);
	MUTATE(query->groupClause, query->groupClause, PGList *);
	MUTATE(query->scatterClause, query->scatterClause, PGList *);
	MUTATE(query->havingQual, query->havingQual, PGNode *);
	MUTATE(query->windowClause, query->windowClause, PGList *);
	MUTATE(query->limitOffset, query->limitOffset, PGNode *);
	MUTATE(query->limitCount, query->limitCount, PGNode *);
	if (!(flags & QTW_IGNORE_CTE_SUBQUERIES))
		MUTATE(query->cteList, query->cteList, PGList *);
	else						/* else copy CTE list as-is */
		query->cteList = (PGList *)copyObject(query->cteList);
	query->rtable = pg_range_table_mutator(query->rtable,
										mutator, context, flags);
	return query;
};

PGNode *
pg_flatten_join_alias_vars_mutator(PGNode *node,
			flatten_join_alias_vars_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, PGVar))
	{
		PGVar		   *var = (PGVar *) node;
		PGRangeTblEntry *rte;
		PGNode	   *newvar;

		/* No change unless Var belongs to a JOIN of the target level */
		if (var->varlevelsup != context->sublevels_up)
			return node;		/* no need to copy, really */
		rte = rt_fetch(var->varno, context->query->rtable);
		if (rte->rtekind != PG_RTE_JOIN)
			return node;
		if (var->varattno == InvalidAttrNumber)
		{
			/* Must expand whole-row reference */
			PGRowExpr    *rowexpr;
			PGList	   *fields = NIL;
			PGList	   *colnames = NIL;
			PGAttrNumber	attnum;
			PGListCell   *lv;
			PGListCell   *ln;

			attnum = 0;
			Assert(list_length(rte->joinaliasvars) == list_length(rte->eref->colnames));
			forboth(lv, rte->joinaliasvars, ln, rte->eref->colnames)
			{
				newvar = (PGNode *) lfirst(lv);
				attnum++;
				/* Ignore dropped columns */
				if (newvar == NULL)
					continue;
				newvar = (PGNode *)copyObject(newvar);

				/*
				 * If we are expanding an alias carried down from an upper
				 * query, must adjust its varlevelsup fields.
				 */
				if (context->sublevels_up != 0)
					PGIncrementVarSublevelsUp(newvar, context->sublevels_up, 0);
				/* Preserve original Var's location, if possible */
				if (IsA(newvar, PGVar))
					((PGVar *) newvar)->location = var->location;
				/* Recurse in case join input is itself a join */
				/* (also takes care of setting inserted_sublink if needed) */
				newvar = pg_flatten_join_alias_vars_mutator(newvar, context);
				fields = lappend(fields, newvar);
				/* We need the names of non-dropped columns, too */
				colnames = lappend(colnames, copyObject((PGNode *) lfirst(ln)));
			}
			rowexpr = makeNode(PGRowExpr);
			rowexpr->args = fields;
			rowexpr->row_typeid = var->vartype;
			rowexpr->row_format = PG_COERCE_IMPLICIT_CAST;
			rowexpr->colnames = colnames;
			rowexpr->location = var->location;

			return (PGNode *) rowexpr;
		}

		/* Expand join alias reference */
		Assert(var->varattno > 0);
		newvar = (PGNode *) list_nth(rte->joinaliasvars, var->varattno - 1);
		Assert(newvar != NULL);
		newvar = (PGNode *)copyObject(newvar);

		/*
		 * If we are expanding an alias carried down from an upper query, must
		 * adjust its varlevelsup fields.
		 */
		if (context->sublevels_up != 0)
			PGIncrementVarSublevelsUp(newvar, context->sublevels_up, 0);

		/* Preserve original Var's location, if possible */
		if (IsA(newvar, PGVar))
			((PGVar *) newvar)->location = var->location;

		/* Recurse in case join input is itself a join */
		newvar = pg_flatten_join_alias_vars_mutator(newvar, context);

		/* Detect if we are adding a sublink to query */
		if (context->possible_sublink && !context->inserted_sublink)
			context->inserted_sublink = checkExprHasSubLink(newvar);

		return newvar;
	}
	// if (IsA(node, PlaceHolderVar))
	// {
	// 	/* Copy the PlaceHolderVar node with correct mutation of subnodes */
	// 	PlaceHolderVar *phv;

	// 	phv = (PlaceHolderVar *) pg_expression_tree_mutator(node,
	// 													 flatten_join_alias_vars_mutator,
	// 													 (void *) context);
	// 	/* now fix PlaceHolderVar's relid sets */
	// 	if (phv->phlevelsup == context->sublevels_up)
	// 	{
	// 		phv->phrels = alias_relid_set(context->query,
	// 									  phv->phrels);
	// 	}
	// 	return (PGNode *) phv;
	// }

	if (IsA(node, PGQuery))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		PGQuery	   *newnode;
		bool		save_inserted_sublink;

		context->sublevels_up++;
		save_inserted_sublink = context->inserted_sublink;
		context->inserted_sublink = ((PGQuery *) node)->hasSubLinks;
		newnode = pg_query_tree_mutator((PGQuery *) node,
									 pg_flatten_join_alias_vars_mutator,
									 (void *) context,
									 QTW_IGNORE_JOINALIASES);
		newnode->hasSubLinks |= context->inserted_sublink;
		context->inserted_sublink = save_inserted_sublink;
		context->sublevels_up--;
		return (PGNode *) newnode;
	}
	/* Already-planned tree not supported */
	Assert(!IsA(node, PGSubPlan));
	/* Shouldn't need to handle these planner auxiliary nodes here */
	// Assert(!IsA(node, SpecialJoinInfo));
	// Assert(!IsA(node, PlaceHolderInfo));
	// Assert(!IsA(node, MinMaxAggInfo));

	return pg_expression_tree_mutator(node, pg_flatten_join_alias_vars_mutator,
								   (void *) context);
};

PGNode *
pg_flatten_join_alias_vars(PGQuery *query, PGNode *node)
{
	flatten_join_alias_vars_context context;

	context.query = query;
	context.sublevels_up = 0;
	/* flag whether join aliases could possibly contain SubLinks */
	context.possible_sublink = query->hasSubLinks;
	/* if hasSubLinks is already true, no need to work hard */
	context.inserted_sublink = query->hasSubLinks;

	return pg_flatten_join_alias_vars_mutator(node, &context);
};