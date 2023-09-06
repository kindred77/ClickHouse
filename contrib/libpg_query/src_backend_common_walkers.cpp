#include "common/common_walkers.hpp"
#include "common/common_equalfuncs.hpp"
#include "common/parser_common.hpp"
#include "pg_functions.hpp"
#include "nodes/nodeFuncs.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb_libpgquery {

PGTargetEntry *
tlist_member(PGNode *node, PGList *targetlist)
{
    PGListCell * temp;

    foreach (temp, targetlist)
    {
        PGTargetEntry * tlentry = (PGTargetEntry *)lfirst(temp);

        Assert(IsA(tlentry, PGTargetEntry))

        if (pg_equal(node, tlentry->expr))
            return tlentry;
    }
    return NULL;
};

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
                    Assert(nodeTag(when) == T_PGCaseExpr)
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
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			//throw ::DB::Exception(ERROR, "unrecognized node type: {}", (int) nodeTag(node));
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
	Assert(query != NULL && IsA(query, PGQuery))

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

// bool
// pg_contain_windowfuncs_walker(PGNode *node, void *context)
// {
// 	if (node == NULL)
// 		return false;
// 	if (IsA(node, PGWindowFunc))
// 		return true;			/* abort the tree traversal and return true */
// 	/* Mustn't recurse into subselects */
// 	return pg_expression_tree_walker(node, (walker_func)pg_contain_windowfuncs_walker,
// 								  (void *) context);
// };

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

// bool
// pg_contain_windowfuncs_walker(PGNode *node, void *context)
// {
// 	if (node == NULL)
// 		return false;
// 	if (IsA(node, PGWindowFunc))
// 		return true;			/* abort the tree traversal and return true */
// 	/* Mustn't recurse into subselects */
// 	return pg_expression_tree_walker(node, (walker_func)pg_contain_windowfuncs_walker,
// 								  (void *) context);
// };

// bool
// pg_contain_windowfuncs(PGNode *node)
// {
// 	/*
// 	 * Must be prepared to start with a Query or a bare expression tree; if
// 	 * it's a Query, we don't want to increment sublevels_up.
// 	 */
// 	return pg_query_or_expression_tree_walker(node,
// 										   (walker_func)pg_contain_windowfuncs_walker,
// 										   NULL,
// 										   0);
// };

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
        PGVar * var = (PGVar *)node;

        if (var->varlevelsup >= context->min_sublevels_up)
            var->varlevelsup += context->delta_sublevels_up;
        return false; /* done here */
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
        PGAggref * agg = (PGAggref *)node;

        if (agg->agglevelsup >= context->min_sublevels_up)
            agg->agglevelsup += context->delta_sublevels_up;
        /* fall through to recurse into argument */
    }
    // if (IsA(node, PlaceHolderVar))
    // {
    //     PlaceHolderVar * phv = (PlaceHolderVar *)node;

    //     if (phv->phlevelsup >= context->min_sublevels_up)
    //         phv->phlevelsup += context->delta_sublevels_up;
    //     /* fall through to recurse into argument */
    // }
    if (IsA(node, PGRangeTblEntry))
    {
        PGRangeTblEntry * rte = (PGRangeTblEntry *)node;

        if (rte->rtekind == PG_RTE_CTE)
        {
            if (rte->ctelevelsup >= context->min_sublevels_up)
                rte->ctelevelsup += context->delta_sublevels_up;

            /*
			* Fix for MPP-19436: in transformGroupedWindows, min_sublevels_up
			* is ignored. For RTE refer to the original query ctelist should
			* all be incremented.
			*/
            if (context->ignore_min_sublevels_up && rte->ctelevelsup == context->min_sublevels_up - 1)
            {
                rte->ctelevelsup += context->delta_sublevels_up;
            }
        }
        return false; /* allow range_table_walker to continue */
    }
    if (IsA(node, PGQuery))
    {
        /* Recurse into subselects */
        bool result;

        context->min_sublevels_up++;
        result = pg_query_tree_walker((PGQuery *)node, (walker_func)PGIncrementVarSublevelsUp_walker, (void *)context, QTW_EXAMINE_RTES);
        context->min_sublevels_up--;
        return result;
    }
    return pg_expression_tree_walker(node, (walker_func)PGIncrementVarSublevelsUp_walker, (void *)context);
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
    pg_query_or_expression_tree_walker(node, (walker_func)PGIncrementVarSublevelsUp_walker, (void *)&context, QTW_EXAMINE_RTES);
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
	//TODO kindred
	//check_stack_depth();

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
				PGDistinctExpr *expr = (PGDistinctExpr *) node;
				PGDistinctExpr *newnode;

				FLATCOPY(newnode, expr, PGDistinctExpr);
				MUTATE(newnode->args, expr->args, PGList *);
				return (PGNode *) newnode;
			}
			break;
		case T_PGNullIfExpr:
			{
				PGNullIfExpr *expr = (PGNullIfExpr *) node;
				PGNullIfExpr *newnode;

				FLATCOPY(newnode, expr, PGNullIfExpr);
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
				//TODO kindred
				//MUTATE(newnode->elemexpr, acoerce->elemexpr, PGExpr *);
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
		// case T_PGTableSampleClause:
		// 	{
		// 		PGTableSampleClause *tsc = (PGTableSampleClause *) node;
		// 		PGTableSampleClause *newnode;

		// 		FLATCOPY(newnode, tsc, PGTableSampleClause);
		// 		MUTATE(newnode->args, tsc->args, PGList *);
		// 		MUTATE(newnode->repeatable, tsc->repeatable, PGExpr *);
		// 		return (PGNode *) newnode;
		// 	}
		// 	break;
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

PGNode * pg_query_or_expression_tree_mutator(
	PGNode * node, 
	PGNode * (*mutator)(duckdb_libpgquery::PGNode *node, void *context),
	void * context, int flags)
{
    if (node && IsA(node, PGQuery))
        return (PGNode *)pg_query_tree_mutator((PGQuery *)node, mutator, context, flags);
    else
        return mutator(node, context);
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
		//TODO kindred
		//MUTATE(newrte->securityQuals, rte->securityQuals, PGList *);
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
	Assert(query != NULL && IsA(query, PGQuery))

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
	//TODO kindred
	//MUTATE(query->scatterClause, query->scatterClause, PGList *);
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

bool checkExprHasSubLink(PGNode * node)
{
    /*
	 * If a Query is passed, examine it --- but we should not recurse into
	 * sub-Queries that are in its rangetable or CTE list.
	 */
    return pg_query_or_expression_tree_walker(node, (walker_func)checkExprHasSubLink_walker, NULL, QTW_IGNORE_RC_SUBQUERIES);
};

bool checkExprHasSubLink_walker(PGNode * node, void * context)
{
    if (node == NULL)
        return false;
    if (IsA(node, PGSubLink))
        return true; /* abort the tree traversal and return true */
    return pg_expression_tree_walker(node, (walker_func)checkExprHasSubLink_walker, context);
};

PGNode *
pg_flatten_join_alias_vars_mutator(PGNode *node,
			void *context_arg)
{
    if (node == NULL)
	{
        return NULL;
	}
	
	flatten_join_alias_vars_context * context = (flatten_join_alias_vars_context *)context_arg;

    if (IsA(node, PGVar))
    {
        PGVar * var = (PGVar *)node;
        PGRangeTblEntry * rte;
        PGNode * newvar;

        /* No change unless Var belongs to a JOIN of the target level */
        if (var->varlevelsup != context->sublevels_up)
            return node; /* no need to copy, really */
        rte = (PGRangeTblEntry *)(context->root_parse_rtable_arrray[var->varno - 1]);
        if (rte->rtekind != PG_RTE_JOIN)
            return node;
        if (var->varattno == InvalidAttrNumber)
        {
            /* Must expand whole-row reference */
            PGRowExpr * rowexpr;
            PGList * fields = NIL;
            PGList * colnames = NIL;
            //PGAttrNumber attnum;
            PGListCell * lv;
            PGListCell * ln;

            //attnum = 0;
            Assert(list_length(rte->joinaliasvars) == list_length(rte->eref->colnames))
            forboth(lv, rte->joinaliasvars, ln, rte->eref->colnames)
            {
                newvar = (PGNode *)lfirst(lv);
                //attnum++;
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
                    ((PGVar *)newvar)->location = var->location;
                /* Recurse in case join input is itself a join */
                /* (also takes care of setting inserted_sublink if needed) */
                newvar = pg_flatten_join_alias_vars_mutator(newvar, context);
                fields = lappend(fields, newvar);
                /* We need the names of non-dropped columns, too */
                colnames = lappend(colnames, copyObject((PGNode *)lfirst(ln)));
            }
            rowexpr = makeNode(PGRowExpr);
            rowexpr->args = fields;
            rowexpr->row_typeid = var->vartype;
            rowexpr->row_format = PG_COERCE_IMPLICIT_CAST;
            rowexpr->colnames = colnames;
            rowexpr->location = var->location;

            return (PGNode *)rowexpr;
        }

        /* Expand join alias reference */
        Assert(var->varattno > 0)
        newvar = (PGNode *)list_nth(rte->joinaliasvars, var->varattno - 1);
        Assert(newvar != NULL)
        newvar = (PGNode *)copyObject(newvar);

        /*
		 * If we are expanding an alias carried down from an upper query, must
		 * adjust its varlevelsup fields.
		 */
        if (context->sublevels_up != 0)
            PGIncrementVarSublevelsUp(newvar, context->sublevels_up, 0);

        /* Preserve original Var's location, if possible */
        if (IsA(newvar, PGVar))
            ((PGVar *)newvar)->location = var->location;

        /* Recurse in case join input is itself a join */
        newvar = pg_flatten_join_alias_vars_mutator(newvar, context);

        /* Detect if we are adding a sublink to query */
        if (context->possible_sublink && !context->inserted_sublink)
            context->inserted_sublink = checkExprHasSubLink(newvar);

        return newvar;
    }
	//TODO kindred
    // if (IsA(node, PlaceHolderVar))
    // {
    //     /* Copy the PlaceHolderVar node with correct mutation of subnodes */
    //     PlaceHolderVar * phv;

    //     phv = (PlaceHolderVar *)expression_tree_mutator(node, flatten_join_alias_vars_mutator, (void *)context);
    //     /* now fix PlaceHolderVar's relid sets */
    //     if (phv->phlevelsup == context->sublevels_up)
    //     {
    //         phv->phrels = alias_relid_set(context->root, phv->phrels);
    //     }
    //     return (Node *)phv;
    // }

    if (IsA(node, PGQuery))
    {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        PGQuery * newnode;
        bool save_inserted_sublink;

        context->sublevels_up++;
        save_inserted_sublink = context->inserted_sublink;
        context->inserted_sublink = ((PGQuery *)node)->hasSubLinks;
        newnode = pg_query_tree_mutator((PGQuery *)node, pg_flatten_join_alias_vars_mutator, (void *)context, QTW_IGNORE_JOINALIASES);
        newnode->hasSubLinks |= context->inserted_sublink;
        context->inserted_sublink = save_inserted_sublink;
        context->sublevels_up--;
        return (PGNode *)newnode;
    }
    /* Already-planned tree not supported */
    Assert(!IsA(node, PGSubPlan))
    /* Shouldn't need to handle these planner auxiliary nodes here */
    Assert(!IsA(node, PGSpecialJoinInfo))
	//TODO kindred
    //Assert(!IsA(node, LateralJoinInfo))
    Assert(!IsA(node, PGPlaceHolderInfo))
    Assert(!IsA(node, PGMinMaxAggInfo))

    return pg_expression_tree_mutator(node, pg_flatten_join_alias_vars_mutator, (void *)context);
};

PGNode **
rtable_to_array(PGList *rtable)
{
    PGListCell * lc;
    int i;
    PGNode ** arr = (PGNode **)palloc(sizeof(PGNode *) * list_length(rtable));

    foreach_with_count(lc, rtable, i)
    {
        arr[i] = (PGNode *)lfirst(lc);
    }

    return arr;
};

PGNode *
pg_flatten_join_alias_vars(PGPlannerInfo *root, PGNode *node)
{
    flatten_join_alias_vars_context context;

    context.root = root;
    context.sublevels_up = 0;
    /* flag whether join aliases could possibly contain SubLinks */
    context.possible_sublink = root->parse->hasSubLinks;
    /* if hasSubLinks is already true, no need to work hard */
    context.inserted_sublink = root->parse->hasSubLinks;

    /*
	 * The following funcation call flatten_join_alias_vars_mutator()
	 * will walk the expr and it will frequently access root
	 * parse tree's rtable using list_nth. When the rtable is huge,
	 * performance is poor. Here we cache the rtable list into array
	 * to achieve random access to speed up a lot when rtable is huge.
	 * See Github issue https://github.com/greenplum-db/gpdb/issues/11379
	 * for details.
	 */
    context.root_parse_rtable_arrray = rtable_to_array(root->parse->rtable);

    return pg_flatten_join_alias_vars_mutator(node, &context);
};

/*
 * flatten_join_alias_var_optimizer
 *	  Replace Vars that reference JOIN outputs with references to the original
 *	  relation variables instead.
 */
PGQuery *
pg_flatten_join_alias_var_optimizer(PGQuery *query, int queryLevel)
{
	PGQuery *queryNew = (PGQuery *) copyObject(query);

	/* Create a PlannerInfo data structure for this subquery */
	PGPlannerInfo *root = makeNode(PGPlannerInfo);
	root->parse = queryNew;
	root->query_level = queryLevel;

	root->glob = makeNode(PGPlannerGlobal);
	//root->glob->boundParams = NULL;
	root->glob->subplans = NIL;
	root->glob->subroots = NIL;
	root->glob->finalrtable = NIL;
	root->glob->relationOids = NIL;
	root->glob->invalItems = NIL;
	root->glob->transientPlan = false;
	root->glob->nParamExec = 0;

	//root->config = DefaultPlannerConfig();
	root->config = new PGPlannerConfig;

	root->parent_root = NULL;
	//root->planner_cxt = CurrentMemoryContext;
	root->init_plans = NIL;

	root->list_cteplaninfo = NIL;
	root->join_info_list = NIL;
	root->append_rel_list = NIL;

	root->hasJoinRTEs = false;

	PGListCell *plc = NULL;
	foreach(plc, queryNew->rtable)
	{
		PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(plc);

		if (rte->rtekind == PG_RTE_JOIN)
		{
			root->hasJoinRTEs = true;
			if (IS_OUTER_JOIN(rte->jointype))
			{
				break;
			}
		}
	}

	/*
	 * Flatten join alias for expression in
	 * 1. targetlist
	 * 2. returningList
	 * 3. having qual
	 * 4. scatterClause
	 * 5. limit offset
	 * 6. limit count
	 * 
	 * We flatten the above expressions since these entries may be moved during the query 
	 * normalization step before algebrization. In contrast, the planner flattens alias 
	 * inside quals to allow predicates involving such vars to be pushed down. 
	 * 
	 * Here we ignore the flattening of quals due to the following reasons:
	 * 1. we assume that the function will be called before Query->DXL translation:
	 * 2. the quals never gets moved from old query to the new top-level query in the 
	 * query normalization phase before algebrization. In other words, the quals hang of 
	 * the same query structure that is now the new derived table.
	 * 3. the algebrizer can resolve the abiquity of join aliases in quals since we maintain 
	 * all combinations of <query level, varno, varattno> to DXL-ColId during Query->DXL translation.
	 * 
	 */

	PGList *targetList = queryNew->targetList;
	if (NIL != targetList)
	{
		queryNew->targetList = (PGList *) pg_flatten_join_alias_vars(root, (PGNode *) targetList);
		pfree(targetList);
	}

	PGList * returningList = queryNew->returningList;
	if (NIL != returningList)
	{
		queryNew->returningList = (PGList *) pg_flatten_join_alias_vars(root, (PGNode *) returningList);
		pfree(returningList);
	}

	PGNode *havingQual = queryNew->havingQual;
	if (NULL != havingQual)
	{
		queryNew->havingQual = pg_flatten_join_alias_vars(root, havingQual);
		pfree(havingQual);
	}

	// PGList *scatterClause = queryNew->scatterClause;
	// if (NIL != scatterClause)
	// {
	// 	queryNew->scatterClause = (PGList *) pg_flatten_join_alias_vars(root, (PGNode *) scatterClause);
	// 	pfree(scatterClause);
	// }

	PGNode *limitOffset = queryNew->limitOffset;
	if (NULL != limitOffset)
	{
		queryNew->limitOffset = pg_flatten_join_alias_vars(root, limitOffset);
		pfree(limitOffset);
	}

	PGList *windowClause = queryNew->windowClause;
	if (NIL != queryNew->windowClause)
	{
		PGListCell *l;

		foreach (l, windowClause)
		{
			PGWindowClause *wc = (PGWindowClause *) lfirst(l);

			if (wc == NULL)
				continue;

			if (wc->startOffset)
				wc->startOffset = pg_flatten_join_alias_vars(root, wc->startOffset);

			if (wc->endOffset)
				wc->endOffset = pg_flatten_join_alias_vars(root, wc->endOffset);
		}
	}

	PGNode *limitCount = queryNew->limitCount;
	if (NULL != limitCount)
	{
		queryNew->limitCount = pg_flatten_join_alias_vars(root, limitCount);
		pfree(limitCount);
	}

    return queryNew;
};

bool cdb_walk_vars_walker(PGNode * node, void * wvwcontext)
{
    // using duckdb_libpgquery::PGNode;
    // using duckdb_libpgquery::PGVar;
    // using duckdb_libpgquery::PGAggref;
    // using duckdb_libpgquery::PGCurrentOfExpr;
    // using duckdb_libpgquery::PGQuery;

    // using duckdb_libpgquery::T_PGVar;
    // using duckdb_libpgquery::T_PGAggref;
    // using duckdb_libpgquery::T_PGCurrentOfExpr;
    // using duckdb_libpgquery::T_PGQuery;

    Cdb_walk_vars_context * ctx = (Cdb_walk_vars_context *)wvwcontext;

    if (node == NULL)
        return false;

    if (IsA(node, PGVar) && ctx->callback_var != NULL)
        return ctx->callback_var((PGVar *)node, ctx->context, ctx->sublevelsup);

    if (IsA(node, PGAggref) && ctx->callback_aggref != NULL)
        return ctx->callback_aggref((PGAggref *)node, ctx->context, ctx->sublevelsup);

    if (IsA(node, PGCurrentOfExpr) && ctx->callback_currentof != NULL)
        return ctx->callback_currentof((PGCurrentOfExpr *)node, ctx->context, ctx->sublevelsup);

    if (IsA(node, PGQuery))
    {
        bool b;

        /* Recurse into subselects */
        ctx->sublevelsup++;
        b = pg_query_tree_walker((PGQuery *)node, (walker_func)cdb_walk_vars_walker, ctx, 0);
        ctx->sublevelsup--;
        return b;
    }
    return pg_expression_tree_walker(node, (walker_func)cdb_walk_vars_walker, ctx);
}; /* cdb_walk_vars_walker */

bool cdb_walk_vars(
    PGNode * node,
    Cdb_walk_vars_callback_Var callback_var,
    Cdb_walk_vars_callback_Aggref callback_aggref,
    Cdb_walk_vars_callback_CurrentOf callback_currentof,
    void * context,
    int levelsup)
{
    Cdb_walk_vars_context ctx;

    ctx.callback_var = callback_var;
    ctx.callback_aggref = callback_aggref;
    ctx.callback_currentof = callback_currentof;
    ctx.context = context;
    ctx.sublevelsup = levelsup;

    /*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment levelsdown.
	 */
    return pg_query_or_expression_tree_walker(node, (walker_func)cdb_walk_vars_walker, &ctx, 0);
}; /* cdb_walk_vars */

/*
 * contain_vars_of_level
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  of the specified query level.
 *
 *	  Returns true if any such Var found.
 *
 * Will recurse into sublinks.  Also, may be invoked directly on a Query.
 */
bool contain_vars_of_level(PGNode * node, int levelsup)
{
    int sublevels_up = levelsup;

    return pg_query_or_expression_tree_walker(node, (walker_func)contain_vars_of_level_walker, (void *)&sublevels_up, 0);
};

bool contain_vars_of_level_walker(PGNode * node, int * sublevels_up)
{
    if (node == NULL)
        return false;
    if (IsA(node, PGVar))
    {
        if (((PGVar *)node)->varlevelsup == *sublevels_up)
            return true; /* abort tree traversal and return true */
        return false;
    }
    if (IsA(node, PGCurrentOfExpr))
    {
        if (*sublevels_up == 0)
            return true;
        return false;
    }
    // if (IsA(node, PGPlaceHolderVar))
    // {
    //     if (((PGPlaceHolderVar *)node)->phlevelsup == *sublevels_up)
    //         return true; /* abort the tree traversal and return true */
    //     /* else fall through to check the contained expr */
    // }
    if (IsA(node, PGQuery))
    {
        /* Recurse into subselects */
        bool result;

        (*sublevels_up)++;
        result = pg_query_tree_walker((PGQuery *)node, (walker_func)contain_vars_of_level_walker, (void *)sublevels_up, 0);
        (*sublevels_up)--;
        return result;
    }
    return pg_expression_tree_walker(node, (walker_func)contain_vars_of_level_walker, (void *)sublevels_up);
};

bool winref_checkspec_walker(duckdb_libpgquery::PGNode * node, void * ctx)
{
	using duckdb_libpgquery::PGWindowFunc;

    winref_check_ctx * ref = (winref_check_ctx *)ctx;

    if (!node)
        return false;
    else if (IsA(node, PGWindowFunc))
    {
        PGWindowFunc * winref = (PGWindowFunc *)node;

        /*
		 * Look at functions pointing to the interesting spec only.
		 */
        if (winref->winref != ref->winref)
            return false;

		//TODO kindred
        // if (winref->windistinct)
        // {
        //     if (ref->has_order)
        //         ereport(
        //             ERROR,
        //             (errcode(ERRCODE_SYNTAX_ERROR),
        //              errmsg("DISTINCT cannot be used with window specification containing an ORDER BY clause"),
        //              parser_errposition(ref->pstate, winref->location)));

        //     if (ref->has_frame)
        //         ereport(
        //             ERROR,
        //             (errcode(ERRCODE_SYNTAX_ERROR),
        //              errmsg("DISTINCT cannot be used with window specification containing a framing clause"),
        //              parser_errposition(ref->pstate, winref->location)));
        // }
#if 0 // FIXME
		/*
		 * Check compatibilities between function's requirement and
		 * window specification by looking up pg_window catalog.
		 */
		if (!ref->has_order || ref->has_frame)
		{
			HeapTuple		tuple;
			Form_pg_window	wf;

			tuple = SearchSysCache1(WINFNOID,
									ObjectIdGetDatum(winref->winfnoid));

			/*
			 * Check only "true" window function.
			 * Otherwise, it must be an aggregate.
			 */
			if (HeapTupleIsValid(tuple))
			{
				wf = (Form_pg_window) GETSTRUCT(tuple);
				if (wf->winrequireorder && !ref->has_order)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("window function \"%s\" requires a window specification with an ordering clause",
									get_func_name(wf->winfnoid)),
								parser_errposition(ref->pstate, winref->location)));

				if (!wf->winallowframe && ref->has_frame)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("window function \"%s\" cannot be used with a framed window specification",
									get_func_name(wf->winfnoid)),
								parser_errposition(ref->pstate, winref->location)));
				ReleaseSysCache(tuple);
			}
		}
#endif
    }

    return pg_expression_tree_walker(node, (walker_func)winref_checkspec_walker, ctx);
};

bool winref_checkspec(PGParseState * pstate, PGList * targetlist, PGIndex winref, bool has_order, bool has_frame)
{
    winref_check_ctx ctx;

    ctx.pstate = pstate;
    ctx.winref = winref;
    ctx.has_order = has_order;
    ctx.has_frame = has_frame;

    return pg_expression_tree_walker((PGNode *)targetlist, (walker_func)winref_checkspec_walker, (void *)&ctx);
};

void parseCheckTableFunctions(PGParseState * pstate, PGQuery * qry)
{
    check_table_func_context context;
    context.parent = NULL;
    pg_query_tree_walker(qry, (walker_func)checkTableFunctions_walker, (void *)&context, 0);
};

bool checkTableFunctions_walker(PGNode * node, check_table_func_context * context)
{
    if (node == NULL)
        return false;

    /* 
	 * TABLE() value expressions are currently only permited as parameters
	 * to table functions called in the FROM clause.
	 */
	//TODO kindred
    // if (IsA(node, PGTableValueExpr))
    // {
    //     if (context->parent && IsA(context->parent, FuncExpr))
    //     {
    //         FuncExpr * parent = (FuncExpr *)context->parent;

    //         /*
	// 		 * This flag is set in addRangeTableEntryForFunction for functions
	// 		 * called as range table entries having TABLE value expressions
	// 		 * as arguments.
	// 		 */
    //         if (parent->is_tablefunc)
    //             return false;

    //         /* Error message could be improved */
    //         ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("table functions must be invoked in FROM clause")));
    //     }
    //     ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid use of TABLE value expression")));
    //     return true; /* not possible, but keeps compiler happy */
    // }

    context->parent = node;
    if (IsA(node, PGQuery))
    {
        return pg_query_tree_walker((PGQuery *)node, (walker_func)checkTableFunctions_walker, (void *)context, 0);
    }
    else
    {
        return pg_expression_tree_walker(node, (walker_func)checkTableFunctions_walker, (void *)context);
    }
};

bool pg_grouping_rewrite_walker(PGNode * node, void * context)
{
    grouping_rewrite_ctx * ctx = (grouping_rewrite_ctx *)context;

    if (node == NULL)
        return false;

    if (IsA(node, PGAConst))
    {
        return false;
    }
    else if (IsA(node, PGAExpr))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }
    else if (IsA(node, PGColumnRef))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }
    else if (IsA(node, PGTypeCast))
    {
        return false;
    }
    else if (IsA(node, PGGroupingFunc))
    {
        PGGroupingFunc * gf = (PGGroupingFunc *)node;
        PGListCell * arg_lc;
        PGList * newargs = NIL;

		//TODO kindred
        //gf->ngrpcols = list_length(ctx->grp_tles);

        /*
		 * For each argument in gf->args, find its position in grp_tles,
		 * and increment its counts. Note that this is a O(n^2) algorithm,
		 * but it should not matter that much.
		 */
        foreach (arg_lc, gf->args)
        {
            long i = 0;
            PGNode * node_lc = (PGNode *)lfirst(arg_lc);
            PGListCell * grp_lc = NULL;

            foreach (grp_lc, ctx->grp_tles)
            {
                PGTargetEntry * grp_tle = (PGTargetEntry *)lfirst(grp_lc);

                if (equal(grp_tle->expr, node_lc))
                    break;
                i++;
            }

            /* Find a column not in GROUP BY clause */
            if (grp_lc == NULL)
            {
                PGRangeTblEntry * rte;
				//TODO kindred
                //const char * attname;
                PGVar * var = (PGVar *)node_lc;

                PGParseState * pstate = ctx->pstate;
                PGIndex levelsup;

                /* Do not allow expressions inside a grouping function. */
                if (IsA(node_lc, PGRowExpr))
                    ereport(ERROR, (errcode(PG_ERRCODE_GROUPING_ERROR), errmsg("row type can not be used inside a grouping function")));

                if (!IsA(node_lc, PGVar))
                    ereport(
                        ERROR, (errcode(PG_ERRCODE_GROUPING_ERROR), errmsg("expression in a grouping function does not appear in GROUP BY")));

                Assert(IsA(node_lc, PGVar))
                Assert(var->varno > 0)

                for (levelsup = var->varlevelsup; levelsup > 0; levelsup--)
                    pstate = pstate->parentParseState;

                Assert(var->varno <= list_length(pstate->p_rtable))

                rte = rt_fetch(var->varno, pstate->p_rtable);
                //attname = get_rte_attribute_name(rte, var->varattno);

                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_GROUPING_ERROR), errmsg("column number is \"%d\" in \"%s\" is not in GROUP BY", rte->eref->aliasname, var->varattno)));
            }

            newargs = lappend(newargs, makeInteger(i));
        }

        /* Free gf->args since we do not need it any more. */
        list_free_deep(gf->args);
        gf->args = newargs;
    }
    else if (IsA(node, PGSortBy))
    {
        /*
		 * When WindowClause leaves the main parser, partition and order
		 * clauses will be lists of SortBy structures. Process them here to
		 * avoid muddying up the expression_tree_walker().
		 */
        PGSortBy * s = (PGSortBy *)node;
        return pg_grouping_rewrite_walker(s->node, context);
    }
    return pg_expression_tree_walker(node, (walker_func)pg_grouping_rewrite_walker, context);
};

bool pg_check_ungrouped_columns_walker(PGNode * node, check_ungrouped_columns_context * context)
{
    PGListCell * gl;

    if (node == NULL)
        return false;
    if (IsA(node, PGConst) || IsA(node, PGParam))
        return false; /* constants are always acceptable */

    if (IsA(node, PGAggref))
    {
        PGAggref * agg = (PGAggref *)node;

        if ((int)agg->agglevelsup == context->sublevels_up)
        {
            /*
			 * If we find an aggregate call of the original level, do not
			 * recurse into its normal arguments, ORDER BY arguments, or
			 * filter; ungrouped vars there are not an error.  But we should
			 * check direct arguments as though they weren't in an aggregate.
			 * We set a special flag in the context to help produce a useful
			 * error message for ungrouped vars in direct arguments.
			 */
            bool result;

            Assert(!context->in_agg_direct_args)
            context->in_agg_direct_args = true;
            result = pg_check_ungrouped_columns_walker((PGNode *)agg->aggdirectargs, context);
            context->in_agg_direct_args = false;
            return result;
        }

        /*
		 * We can skip recursing into aggregates of higher levels altogether,
		 * since they could not possibly contain Vars of concern to us (see
		 * transformAggregateCall).  We do need to look at aggregates of lower
		 * levels, however.
		 */
        if ((int)agg->agglevelsup > context->sublevels_up)
            return false;
    }

    /*
	 * If we have any GROUP BY items that are not simple Vars, check to see if
	 * subexpression as a whole matches any GROUP BY item. We need to do this
	 * at every recursion level so that we recognize GROUPed-BY expressions
	 * before reaching variables within them. But this only works at the outer
	 * query level, as noted above.
	 */
    if (context->have_non_var_grouping && context->sublevels_up == 0)
    {
        foreach (gl, context->groupClauses)
        {
            if (pg_equal(node, lfirst(gl)))
                return false; /* acceptable, do not descend more */
        }
    }

    /*
	 * If we have an ungrouped Var of the original query level, we have a
	 * failure.  Vars below the original query level are not a problem, and
	 * neither are Vars from above it.  (If such Vars are ungrouped as far as
	 * their own query level is concerned, that's someone else's problem...)
	 */
    if (IsA(node, PGVar))
    {
        PGVar * var = (PGVar *)node;
        PGRangeTblEntry * rte;
		//TODO kindred
        //const char * attname;

        if (var->varlevelsup != context->sublevels_up)
            return false; /* it's not local to my query, ignore */

        /*
		 * Check for a match, if we didn't do it above.
		 */
        if (!context->have_non_var_grouping || context->sublevels_up != 0)
        {
            foreach (gl, context->groupClauses)
            {
                PGVar * gvar = (PGVar *)lfirst(gl);

                if (IsA(gvar, PGVar) && gvar->varno == var->varno && gvar->varattno == var->varattno && gvar->varlevelsup == 0)
                    return false; /* acceptable, we're okay */
            }
        }

        /*
		 * Check whether the Var is known functionally dependent on the GROUP
		 * BY columns.  If so, we can allow the Var to be used, because the
		 * grouping is really a no-op for this table.  However, this deduction
		 * depends on one or more constraints of the table, so we have to add
		 * those constraints to the query's constraintDeps list, because it's
		 * not semantically valid anymore if the constraint(s) get dropped.
		 * (Therefore, this check must be the last-ditch effort before raising
		 * error: we don't want to add dependencies unnecessarily.)
		 *
		 * Because this is a pretty expensive check, and will have the same
		 * outcome for all columns of a table, we remember which RTEs we've
		 * already proven functional dependency for in the func_grouped_rels
		 * list.  This test also prevents us from adding duplicate entries to
		 * the constraintDeps list.
		 */
        if (list_member_int(*context->func_grouped_rels, var->varno))
            return false; /* previously proven acceptable */

        Assert(var->varno > 0 && (int)var->varno <= list_length(context->pstate->p_rtable))
        rte = rt_fetch(var->varno, context->pstate->p_rtable);
        if (rte->rtekind == PG_RTE_RELATION)
        {
			//TODO kindred
            // if (check_functional_grouping(rte->relid, var->varno, 0, context->groupClauses, &context->qry->constraintDeps))
            // {
            //     *context->func_grouped_rels = lappend_int(*context->func_grouped_rels, var->varno);
            //     return false; /* acceptable */
            // }
        }

        /* Found an ungrouped local variable; generate error message */
        //attname = get_rte_attribute_name(rte, var->varattno);
        if (context->sublevels_up == 0)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_GROUPING_ERROR),
                 errmsg(
                     "column number is \"%d\" in \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
                     var->varattno,
					 rte->eref->aliasname),
                 context->in_agg_direct_args ? errdetail("Direct arguments of an ordered-set aggregate must use only grouped columns.") : 0,
                 parser_errposition(context->pstate, var->location)));
        else
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_GROUPING_ERROR),
                 errmsg("subquery uses ungrouped column number is \"%d\" in \"%s\" from outer query", var->varattno, rte->eref->aliasname),
                 parser_errposition(context->pstate, var->location)));
    }

    if (IsA(node, PGQuery))
    {
        /* Recurse into subselects */
        bool result;

        context->sublevels_up++;
        result = pg_query_tree_walker((PGQuery *)node, (walker_func)pg_check_ungrouped_columns_walker, (void *)context, 0);
        context->sublevels_up--;
        return result;
    }
    return pg_expression_tree_walker(node, (walker_func)pg_check_ungrouped_columns_walker, (void *)context);
};

bool
pg_checkExprHasGroupExtFuncs_walker(PGNode *node, checkHasGroupExtFuncs_context *context)
{
    if (node == NULL)
        return false;
	//TODO kindred
    if (IsA(node, PGGroupingFunc)/*  || IsA(node, PGGroupId) */)
    {
        /* XXX do GroupingFunc or GroupId need 'levelsup'? */
        return true; /* abort the tree traversal and return true */
    }
    if (IsA(node, PGQuery))
    {
        /* Recurse into subselects */
        bool result;

        context->sublevels_up++;
        result = pg_query_tree_walker((PGQuery *)node, (walker_func)pg_checkExprHasGroupExtFuncs_walker, (void *)context, 0);
        context->sublevels_up--;
        return result;
    }
    return pg_expression_tree_walker(node, (walker_func)pg_checkExprHasGroupExtFuncs_walker, (void *)context);
};

void pg_get_sortgroupclauses_tles_recurse(PGList * clauses, PGList * targetList,
	PGList ** tles, PGList ** sortops,
	PGList ** eqops)
{
    PGListCell * lc;
    PGListCell * lc_sortop;
    PGListCell * lc_eqop;
    PGList * sub_grouping_tles = NIL;
    PGList * sub_grouping_sortops = NIL;
    PGList * sub_grouping_eqops = NIL;

    foreach (lc, clauses)
    {
        PGNode * node = (PGNode *)lfirst(lc);

        if (node == NULL)
            continue;

        if (IsA(node, PGSortGroupClause))
        {
            PGSortGroupClause * sgc = (PGSortGroupClause *)node;
            PGTargetEntry * tle = get_sortgroupclause_tle(sgc, targetList);

            if (!list_member(*tles, tle))
            {
                *tles = lappend(*tles, tle);
                *sortops = lappend_oid(*sortops, sgc->sortop);
                *eqops = lappend_oid(*eqops, sgc->eqop);
            }
        }
        else if (IsA(node, PGList))
        {
            pg_get_sortgroupclauses_tles_recurse((PGList *)node, targetList, tles, sortops, eqops);
        }
		//TODO kindred
        // else if (IsA(node, GroupingClause))
        // {
        //     /* GroupingClauses are collected into separate list */
        //     get_sortgroupclauses_tles_recurse(
        //         ((GroupingClause *)node)->groupsets, targetList, &sub_grouping_tles, &sub_grouping_sortops, &sub_grouping_eqops);
        // }
        else
            elog(ERROR, "unrecognized node type in list of sort/group clauses: %d", (int)nodeTag(node));
    }

    /*
	 * Put SortGroupClauses before GroupingClauses.
	 */
    forthree(lc, sub_grouping_tles, lc_sortop, sub_grouping_sortops, lc_eqop, sub_grouping_eqops)
    {
        if (!list_member(*tles, lfirst(lc)))
        {
            *tles = lappend(*tles, lfirst(lc));
            *sortops = lappend_oid(*sortops, lfirst_oid(lc_sortop));
            *eqops = lappend_oid(*eqops, lfirst_oid(lc_eqop));
        }
    }
};

void
pg_get_sortgroupclauses_tles(PGList *clauses, PGList *targetList,
						  PGList **tles, PGList **sortops, PGList **eqops)
{
    *tles = NIL;
    *sortops = NIL;
    *eqops = NIL;

    pg_get_sortgroupclauses_tles_recurse(clauses, targetList, tles, sortops, eqops);
};

bool maxSortGroupRef_walker(PGNode *node, maxSortGroupRef_context *cxt)
{
    if (node == NULL)
        return false;

    if (IsA(node, PGTargetEntry))
    {
        PGTargetEntry * tle = (PGTargetEntry *)node;
        if (tle->ressortgroupref > cxt->maxsgr)
            cxt->maxsgr = tle->ressortgroupref;

        return maxSortGroupRef_walker((PGNode *)tle->expr, cxt);
    }

    /* Aggref nodes don't nest, so we can treat them here without recurring
	 * further.
	 */

    if (IsA(node, PGAggref))
    {
        PGAggref * ref = (PGAggref *)node;

        if (cxt->include_orderedagg)
        {
            ListCell * lc;

            foreach (lc, ref->aggorder)
            {
                PGSortGroupClause * sort = (PGSortGroupClause *)lfirst(lc);
                Assert(IsA(sort, PGSortGroupClause))
                Assert(sort->tleSortGroupRef != 0)
                if (sort->tleSortGroupRef > cxt->maxsgr)
                    cxt->maxsgr = sort->tleSortGroupRef;
            }
        }
        return false;
    }

    return pg_expression_tree_walker(node, (walker_func)maxSortGroupRef_walker, cxt);
};

PGIndex maxSortGroupRef(PGList *targetlist, bool include_orderedagg)
{
    maxSortGroupRef_context context;
    context.maxsgr = 0;
    context.include_orderedagg = include_orderedagg;

    if (targetlist != NIL)
    {
        if (!IsA(targetlist, PGList) || !IsA(linitial(targetlist), PGTargetEntry))
            elog(ERROR, "non-targetlist argument supplied");

        maxSortGroupRef_walker((PGNode *)targetlist, &context);
    }

    return context.maxsgr;
};

char * generate_positional_name(PGAttrNumber attrno)
{
	int rc = 0;
	char buf[NAMEDATALEN];

	rc = snprintf(buf, sizeof(buf),
				  "att_%d", attrno );
	if ( rc == EOF || rc < 0 || rc >=sizeof(buf) )
	{
		ereport(ERROR,
				(errcode(PG_ERRCODE_INTERNAL_ERROR),
				 errmsg("can't generate internal attribute name")));
	}
	return pstrdup(buf);
};

PGVar * var_for_gw_expr(grouped_window_ctx * ctx, PGNode * expr, bool force)
{
    PGVar * var = NULL;
    PGTargetEntry * tle = tlist_member(expr, ctx->subtlist);

    if (tle == NULL && force)
    {
        tle = makeNode(PGTargetEntry);
        ctx->subtlist = lappend(ctx->subtlist, tle);
        tle->expr = (PGExpr *)expr;
        tle->resno = list_length(ctx->subtlist);
        /* See comment in grouped_window_mutator for why level 3 is appropriate. */
        if (ctx->call_depth == 3 && ctx->tle != NULL && ctx->tle->resname != NULL)
        {
            tle->resname = pstrdup(ctx->tle->resname);
        }
        else
        {
            tle->resname = generate_positional_name(tle->resno);
        }
        tle->ressortgroupref = 0;
        tle->resorigtbl = 0;
        tle->resorigcol = 0;
        tle->resjunk = false;
    }

    if (tle != NULL)
    {
        var = makeNode(PGVar);
        var->varno = 1; /* one and only */
        var->varattno = tle->resno; /* by construction */
        var->vartype = exprType((PGNode *)tle->expr);
        var->vartypmod = exprTypmod((PGNode *)tle->expr);
        var->varcollid = exprCollation((PGNode *)tle->expr);
        var->varlevelsup = 0;
        var->varnoold = 1;
        var->varoattno = tle->resno;
        var->location = 0;
    }

    return var;
};

PGList*
generate_alternate_vars(PGVar *invar, grouped_window_ctx *ctx)
{
    PGList * rtable = ctx->subrtable;
    PGRangeTblEntry * inrte;
    PGList * alternates = NIL;

    Assert(IsA(invar, PGVar))

    inrte = rt_fetch(invar->varno, rtable);

    if (inrte->rtekind == PG_RTE_JOIN)
    {
        PGNode * ja = (PGNode *)list_nth(inrte->joinaliasvars, invar->varattno - 1);

        /* Though Node types other than Var (e.g., CoalesceExpr or Const) may occur
		 * as joinaliasvars, we ignore them.
		 */
        if (IsA(ja, PGVar))
        {
            alternates = lappend(alternates, copyObject(ja));
        }
    }
    else
    {
        PGListCell * jlc;
        PGIndex varno = 0;

        foreach (jlc, rtable)
        {
            PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(jlc);

            varno++; /* This RTE's varno */

            if (rte->rtekind == PG_RTE_JOIN)
            {
                PGListCell * alc;
                PGAttrNumber attno = 0;

                foreach (alc, rte->joinaliasvars)
                {
                    PGListCell * tlc;
                    PGNode * altnode = (PGNode *)lfirst(alc);
                    PGVar * altvar = (PGVar *)altnode;

                    attno++; /* This attribute's attno in its join RTE */

                    if (!IsA(altvar, PGVar) || !equal(invar, altvar))
                        continue;

                    /* Look for a matching Var in the target list. */

                    foreach (tlc, ctx->subtlist)
                    {
                        PGTargetEntry * tle = (PGTargetEntry *)lfirst(tlc);
                        PGVar * v = (PGVar *)tle->expr;

                        if (IsA(v, PGVar) && v->varno == varno && v->varattno == attno)
                        {
                            alternates = lappend(alternates, tle->expr);
                        }
                    }
                }
            }
        }
    }
    return alternates;
};

PGNode* grouped_window_mutator(PGNode *node, void *context)
{
    PGNode * result = NULL;

    grouped_window_ctx * ctx = (grouped_window_ctx *)context;

    if (!node)
        return result;

    ctx->call_depth++;

    if (IsA(node, PGTargetEntry))
    {
        PGTargetEntry * tle = (PGTargetEntry *)node;
        PGTargetEntry * new_tle = makeNode(PGTargetEntry);

        /* Copy the target entry. */
        new_tle->resno = tle->resno;
        if (tle->resname == NULL)
        {
            new_tle->resname = generate_positional_name(new_tle->resno);
        }
        else
        {
            new_tle->resname = pstrdup(tle->resname);
        }
        new_tle->ressortgroupref = tle->ressortgroupref;
        new_tle->resorigtbl = InvalidOid;
        new_tle->resorigcol = 0;
        new_tle->resjunk = tle->resjunk;

        /* This is pretty shady, but we know our call pattern.  The target
		 * list is at level 1, so we're interested in target entries at level
		 * 2.  We record them in context so var_for_gw_expr can maybe make a better
		 * than default choice of alias.
		 */
        if (ctx->call_depth == 2)
        {
            ctx->tle = tle;
        }
        else
        {
            ctx->tle = NULL;
        }

        new_tle->expr = (PGExpr *)grouped_window_mutator((PGNode *)tle->expr, ctx);

        ctx->tle = NULL;
        result = (PGNode *)new_tle;
    }
	//TODO kindred
    else if (IsA(node, PGAggref) || IsA(node, PGGroupingFunc)/*  || IsA(node, PGGroupId) */)
    {
        /* Aggregation expression */
        result = (PGNode *)var_for_gw_expr(ctx, node, true);
    }
    else if (IsA(node, PGVar))
    {
        PGVar * var = (PGVar *)node;

        /* Since this is a Var (leaf node), we must be able to mutate it,
		 * else we can't finish the transformation and must give up.
		 */
        result = (PGNode *)var_for_gw_expr(ctx, node, false);

        if (!result)
        {
            PGList * altvars = generate_alternate_vars(var, ctx);
            PGListCell * lc;
            foreach (lc, altvars)
            {
                result = (PGNode *)var_for_gw_expr(ctx, (PGNode *)lfirst(lc), false);
                if (result)
                    break;
            }
        }

        if (!result)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WINDOWING_ERROR),
                 errmsg("unresolved grouping key in window query"),
                 errhint("You might need to use explicit aliases and/or to refer to grouping keys in the same way throughout the query.")));
    }
    else if (IsA(node, PGSubLink))
    {
        /* put the subquery into Q'' */
        result = (PGNode *)var_for_gw_expr(ctx, node, true /* force */);
    }
    else
    {
        /* Grouping expression; may not find one. */
        result = (PGNode *)var_for_gw_expr(ctx, node, false /* force */);
    }


    if (!result)
    {
        result = pg_expression_tree_mutator(node, grouped_window_mutator, ctx);
    }

    ctx->call_depth--;
    return result;
};

void IncrementVarSublevelsUpInTransformGroupedWindows(PGNode * node, int delta_sublevels_up, int min_sublevels_up)
{
    IncrementVarSublevelsUp_context context;

    context.delta_sublevels_up = delta_sublevels_up;
    context.min_sublevels_up = min_sublevels_up;
    context.ignore_min_sublevels_up = true;

    /*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
    pg_query_or_expression_tree_walker(node, (walker_func)PGIncrementVarSublevelsUp_walker, (void *)&context, QTW_EXAMINE_RTES);
};

bool
pg_find_nodes_walker(PGNode *node, pg_find_nodes_context *context)
{
    if (NULL == node)
    {
        return false;
    }

    if (IsA(node, PGQuery))
    {
        /* Recurse into subselects */
        return pg_query_tree_walker((PGQuery *)node, (walker_func)pg_find_nodes_walker, (void *)context, 0 /* flags */);
    }

    int i = 0;
    for (const auto& node_tag : context->nodeTags)
    {
        if (nodeTag(node) == node_tag)
        {
            context->foundNode = i;
            return true;
        }

        i++;
    }

    return pg_expression_tree_walker(node, (walker_func)pg_find_nodes_walker, (void *)context);
};

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
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregate function calls cannot contain set-returning function calls"),
					 errhint("You might be able to move the set-returning function into a LATERAL FROM item.")));
		}
		if (IsA(node, PGWindowFunc))
		{
			parser_errposition(context->pstate,
										((PGWindowFunc *) node)->location);
			ereport(ERROR,
					(errcode(PG_ERRCODE_GROUPING_ERROR),
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

}
