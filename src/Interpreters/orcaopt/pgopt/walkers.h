#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>

typedef struct
{
	int			sublevels_up;
} contain_aggs_of_level_context;

/*
 * Collation strength (the SQL standard calls this "derivation").  Order is
 * chosen to allow comparisons to work usefully.  Note: the standard doesn't
 * seem to distinguish between NONE and CONFLICT.
 */
typedef enum
{
	COLLATE_NONE,				/* expression is of a noncollatable datatype */
	COLLATE_IMPLICIT,			/* collation was derived implicitly */
	COLLATE_CONFLICT,			/* we had a conflict of implicit collations */
	COLLATE_EXPLICIT			/* collation was derived explicitly */
} CollateStrength;

typedef struct
{
	PGParseState *pstate;			/* parse state (for error reporting) */
	Oid			collation;		/* OID of current collation, if any */
	CollateStrength strength;	/* strength of current collation choice */
	int			location;		/* location of expr that set collation */
	/* Remaining fields are only valid when strength == COLLATE_CONFLICT */
	Oid			collation2;		/* OID of conflicting collation */
	int			location2;		/* location of expr that set collation2 */
} assign_collations_context;

typedef bool (*walker_func) (duckdb_libpgquery::PGNode *node, assign_collations_context *context);

bool
    expression_tree_walker(duckdb_libpgquery::PGNode *node,
					   walker_func walker,
					   void *context)
{
    using duckdb_libpgquery::PGListCell;
    using duckdb_libpgquery::PGAggref;
    using duckdb_libpgquery::PGNode;
    using duckdb_libpgquery::PGList;
    using duckdb_libpgquery::PGWindowFunc;
    using duckdb_libpgquery::PGArrayRef;
    using duckdb_libpgquery::PGFuncExpr;
    using duckdb_libpgquery::PGNamedArgExpr;
    using duckdb_libpgquery::PGOpExpr;
    using duckdb_libpgquery::PGScalarArrayOpExpr;
    using duckdb_libpgquery::PGBoolExpr;
    using duckdb_libpgquery::PGSubLink;
    using duckdb_libpgquery::PGSubPlan;
    using duckdb_libpgquery::PGAlternativeSubPlan;
    using duckdb_libpgquery::PGFieldSelect;
    using duckdb_libpgquery::PGFieldStore;
    using duckdb_libpgquery::PGRelabelType;
    using duckdb_libpgquery::PGCoerceViaIO;
    using duckdb_libpgquery::PGArrayCoerceExpr;
    using duckdb_libpgquery::PGConvertRowtypeExpr;
    using duckdb_libpgquery::PGCollateExpr;
    using duckdb_libpgquery::PGCaseExpr;
    using duckdb_libpgquery::PGCaseWhen;
    using duckdb_libpgquery::PGArrayExpr;
    using duckdb_libpgquery::PGRowExpr;
    using duckdb_libpgquery::PGRowCompareExpr;
    using duckdb_libpgquery::PGCoalesceExpr;
    using duckdb_libpgquery::PGMinMaxExpr;
    using duckdb_libpgquery::PGNullTest;
    using duckdb_libpgquery::PGBooleanTest;
    using duckdb_libpgquery::PGCoerceToDomain;
    using duckdb_libpgquery::PGTargetEntry;
    using duckdb_libpgquery::PGCommonTableExpr;
    using duckdb_libpgquery::PGFromExpr;
    using duckdb_libpgquery::PGJoinExpr;
    using duckdb_libpgquery::PGRangeTblFunction;
    using duckdb_libpgquery::PGWindowDef;
    using duckdb_libpgquery::PGTypeCast;
    using duckdb_libpgquery::PGWindowClause;

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
		case duckdb_libpgquery::T_PGVar:
		case duckdb_libpgquery::T_PGConst:
		case duckdb_libpgquery::T_PGParam:
		case duckdb_libpgquery::T_PGCoerceToDomainValue:
		case duckdb_libpgquery::T_PGCaseTestExpr:
		case duckdb_libpgquery::T_PGSetToDefault:
		case duckdb_libpgquery::T_PGCurrentOfExpr:
		case duckdb_libpgquery::T_PGRangeTblRef:
		case duckdb_libpgquery::T_PGSortGroupClause:
		// case duckdb_libpgquery::T_PGDMLActionExpr:
		// case duckdb_libpgquery::T_PGPartSelectedExpr:
		// case duckdb_libpgquery::T_PGPartDefaultExpr:
		// case duckdb_libpgquery::T_PGPartBoundExpr:
		// case duckdb_libpgquery::T_PGPartBoundInclusionExpr:
		// case duckdb_libpgquery::T_PGPartBoundOpenExpr:
		// case duckdb_libpgquery::T_PGPartListRuleExpr:
		// case duckdb_libpgquery::T_PGPartListNullTestExpr:
			/* primitive node types with no expression subnodes */
			break;
		// case duckdb_libpgquery::T_PGWithCheckOption:
		// 	return walker(((PGWithCheckOption *) node)->qual, context);
		case duckdb_libpgquery::T_PGAggref:
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
				if (walker((PGNode *) expr->aggfilter, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGWindowFunc:
			{
				PGWindowFunc   *expr = (PGWindowFunc *) node;

				/* recurse directly on explicit arg List */
				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
				if (walker((PGNode *) expr->aggfilter, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGArrayRef:
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
				if (walker(reinterpret_cast<PGNode*>(aref->refexpr),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(reinterpret_cast<PGNode*>(aref->refassgnexpr),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGFuncExpr:
			{
				PGFuncExpr   *expr = (PGFuncExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGNamedArgExpr:
			return walker(reinterpret_cast<PGNode*>(((PGNamedArgExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGOpExpr:
		case duckdb_libpgquery::T_PGDistinctExpr:	/* struct-equivalent to OpExpr */
		case duckdb_libpgquery::T_PGNullIfExpr:		/* struct-equivalent to OpExpr */
			{
				PGOpExpr	   *expr = (PGOpExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGScalarArrayOpExpr:
			{
				PGScalarArrayOpExpr *expr = (PGScalarArrayOpExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGBoolExpr:
			{
				PGBoolExpr   *expr = (PGBoolExpr *) node;

				if (expression_tree_walker((PGNode *) expr->args,
										   walker, context))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGSubLink:
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
		case duckdb_libpgquery::T_PGSubPlan:
			{
				PGSubPlan    *subplan = (PGSubPlan *) node;

				/* recurse into the testexpr, but not into the Plan */
				if (walker(subplan->testexpr, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				/* also examine args list */
				if (expression_tree_walker((PGNode *) subplan->args,
										   walker, context))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGAlternativeSubPlan:
			return walker(reinterpret_cast<PGNode*>(((PGAlternativeSubPlan *) node)->subplans), 
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGFieldSelect:
			return walker(reinterpret_cast<PGNode*>(((PGFieldSelect *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGFieldStore:
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
		case duckdb_libpgquery::T_PGRelabelType:
			return walker(reinterpret_cast<PGNode*>(((PGRelabelType *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGCoerceViaIO:
			return walker(reinterpret_cast<PGNode*>(((PGCoerceViaIO *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGArrayCoerceExpr:
			return walker(reinterpret_cast<PGNode*>(((PGArrayCoerceExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGConvertRowtypeExpr:
			return walker(reinterpret_cast<PGNode*>(((PGConvertRowtypeExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGCollateExpr:
			return walker(reinterpret_cast<PGNode*>(((PGCollateExpr *) node)->arg), 
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGCaseExpr:
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
                    Assert(nodeTag(when) == duckdb_libpgquery::T_PGCaseExpr);
					if (walker(reinterpret_cast<PGNode*>(when->expr), reinterpret_cast<assign_collations_context*>(context)))
						return true;
					if (walker(reinterpret_cast<PGNode*>(when->result), reinterpret_cast<assign_collations_context*>(context)))
						return true;
				}
				if (walker(reinterpret_cast<PGNode*>(caseexpr->defresult), reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGArrayExpr:
			return walker(reinterpret_cast<PGNode*>(((PGArrayExpr *) node)->elements),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGRowExpr:
			/* Assume colnames isn't interesting */
			return walker(reinterpret_cast<PGNode*>(((PGRowExpr *) node)->args),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGRowCompareExpr:
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
		case duckdb_libpgquery::T_PGCoalesceExpr:
			return walker(reinterpret_cast<PGNode*>(((PGCoalesceExpr *) node)->args),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGMinMaxExpr:
			return walker(reinterpret_cast<PGNode*>(((PGMinMaxExpr *) node)->args),
                reinterpret_cast<assign_collations_context*>(context));
		// case duckdb_libpgquery::T_PGXmlExpr:
		// 	{
		// 		PGXmlExpr    *xexpr = (PGXmlExpr *) node;

		// 		if (walker(xexpr->named_args, context))
		// 			return true;
		// 		/* we assume walker doesn't care about arg_names */
		// 		if (walker(xexpr->args, context))
		// 			return true;
		// 	}
		// 	break;
		case duckdb_libpgquery::T_PGNullTest:
			return walker(reinterpret_cast<PGNode*>(((PGNullTest *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGBooleanTest:
			return walker(reinterpret_cast<PGNode*>(((PGBooleanTest *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGCoerceToDomain:
			return walker(reinterpret_cast<PGNode*>(((PGCoerceToDomain *) node)->arg),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGTargetEntry:
			return walker(reinterpret_cast<PGNode*>(((PGTargetEntry *) node)->expr),
                reinterpret_cast<assign_collations_context*>(context));
		case duckdb_libpgquery::T_PGQuery:
			/* Do nothing with a sub-Query, per discussion above */
			break;
		case duckdb_libpgquery::T_PGCommonTableExpr:
			{
				PGCommonTableExpr *cte = (PGCommonTableExpr *) node;

				/*
				 * Invoke the walker on the CTE's Query node, so it can
				 * recurse into the sub-query if it wants to.
				 */
				return walker(cte->ctequery, reinterpret_cast<assign_collations_context*>(context));
			}
			break;
		case duckdb_libpgquery::T_PGList:
			foreach(temp, (PGList *) node)
			{
				if (walker((PGNode *) lfirst(temp), reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGFromExpr:
			{
				PGFromExpr   *from = (PGFromExpr *) node;

				if (walker(reinterpret_cast<PGNode*>(from->fromlist),
                    reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker(from->quals, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGJoinExpr:
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
		// case duckdb_libpgquery::T_PGSetOperationStmt:
		// 	{
		// 		PGSetOperationStmt *setop = (PGSetOperationStmt *) node;

		// 		if (walker(setop->larg, context))
		// 			return true;
		// 		if (walker(setop->rarg, context))
		// 			return true;

		// 		/* groupClauses are deemed uninteresting */
		// 	}
		// 	break;
		// case duckdb_libpgquery::T_PGPlaceHolderVar:
		// 	return walker(((PGPlaceHolderVar *) node)->phexpr, context);
		// case duckdb_libpgquery::T_PGAppendRelInfo:
		// 	{
		// 		PGAppendRelInfo *appinfo = (PGAppendRelInfo *) node;

		// 		if (expression_tree_walker((PGNode *) appinfo->translated_vars,
		// 								   walker, context))
		// 			return true;
		// 	}
		// 	break;
		// case duckdb_libpgquery::T_PGPlaceHolderInfo:
		// 	return walker(((PGPlaceHolderInfo *) node)->ph_var, context);
		case duckdb_libpgquery::T_PGRangeTblFunction:
			return walker(((PGRangeTblFunction *) node)->funcexpr, 
                reinterpret_cast<assign_collations_context*>(context));

		// case duckdb_libpgquery::T_PGGroupingClause:
		// 	{
		// 		PGGroupingClause *g = (PGGroupingClause *) node;
		// 		if (expression_tree_walker((PGNode *)g->groupsets, walker,
		// 			context))
		// 			return true;
		// 	}
		// 	break;
		case duckdb_libpgquery::T_PGGroupingFunc:
			break;
		// case duckdb_libpgquery::T_PGGrouping:
		// case duckdb_libpgquery::T_PGGroupId:
		// 	{
		// 		/* do nothing */
		// 	}
		// 	break;
		case duckdb_libpgquery::T_PGWindowDef:
			{
				PGWindowDef  *wd = (PGWindowDef *) node;

				if (expression_tree_walker((PGNode *) wd->partitionClause, walker,
										   context))
					return true;
				if (expression_tree_walker((PGNode *) wd->orderClause, walker,
										   context))
					return true;
				if (walker((PGNode *) wd->startOffset, reinterpret_cast<assign_collations_context*>(context)))
					return true;
				if (walker((PGNode *) wd->endOffset, reinterpret_cast<assign_collations_context*>(context)))
					return true;
			}
			break;
		case duckdb_libpgquery::T_PGTypeCast:
			{
				PGTypeCast *tc = (PGTypeCast *)node;

				if (expression_tree_walker((PGNode*) tc->arg, walker, context))
					return true;
			}
			break;
		// case duckdb_libpgquery::T_PGTableValueExpr:
		// 	{
		// 		PGTableValueExpr *expr = (PGTableValueExpr *) node;

		// 		return walker(expr->subquery, context);
		// 	}
		// 	break;
		case duckdb_libpgquery::T_PGWindowClause:
			{
				PGWindowClause *wc = (PGWindowClause *) node;

				if (expression_tree_walker((PGNode *) wc->partitionClause, walker,
										   context))
					return true;
				if (expression_tree_walker((PGNode *) wc->orderClause, walker,
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
range_table_walker(duckdb_libpgquery::PGList *rtable,
				   walker_func walker,
				   void *context,
				   int flags)
{
	using duckdb_libpgquery::PGListCell;
	using duckdb_libpgquery::PGRangeTblEntry;
	using duckdb_libpgquery::PGRTEKind;

	PGListCell   *rt;

	foreach(rt, rtable)
	{
		PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(rt);

		/* For historical reasons, visiting RTEs is not the default */
		if (flags & QTW_EXAMINE_RTES)
			if (walker(rte, context))
				return true;

		switch (rte->rtekind)
		{
			case PGRTEKind::PG_RTE_RELATION:
			case PGRTEKind::PG_RTE_VOID:
			case PGRTEKind::PG_RTE_CTE:
				/* nothing to do */
				break;
			case PGRTEKind::PG_RTE_SUBQUERY:
				if (!(flags & QTW_IGNORE_RT_SUBQUERIES))
					if (walker(rte->subquery, context))
						return true;
				break;
			case PGRTEKind::PG_RTE_JOIN:
				if (!(flags & QTW_IGNORE_JOINALIASES))
					if (walker(rte->joinaliasvars, context))
						return true;
				break;
			case PGRTEKind::PG_RTE_FUNCTION:
				if (walker(rte->functions, context))
					return true;
				break;
			case PGRTEKind::PG_RTE_TABLEFUNCTION:
				if (walker(rte->subquery, context))
					return true;
				if (walker(rte->functions, context))
					return true;
				break;
			case PGRTEKind::PG_RTE_VALUES:
				if (walker(rte->values_lists, context))
					return true;
				break;
		}

		if (walker(rte->securityQuals, context))
			return true;
	}
	return false;
};

bool
query_tree_walker(duckdb_libpgquery::PGQuery *query,
				  walker_func walker,
				  void *context,
				  int flags)
{
	using duckdb_libpgquery::PGQuery;
	using duckdb_libpgquery::PGNode;
	using duckdb_libpgquery::PGWindowClause;
	using duckdb_libpgquery::PGCopyStmt;
	using duckdb_libpgquery::PGExplainStmt;
	using duckdb_libpgquery::PGListCell;
	using duckdb_libpgquery::PGPrepareStmt;
	using duckdb_libpgquery::PGViewStmt;

	Assert(query != NULL && IsA(query, PGQuery));

	/*
	 * We don't walk any utilityStmt here. However, we can't easily assert
	 * that it is absent, since there are at least two code paths by which
	 * action statements from CREATE RULE end up here, and NOTIFY is allowed
	 * in a rule action.
	 */

	if (walker((PGNode *) query->targetList, context))
		return true;
	if (walker((PGNode *) query->withCheckOptions, context))
		return true;
	if (walker((PGNode *) query->returningList, context))
		return true;
	if (walker((PGNode *) query->jointree, context))
		return true;
	if (walker(query->setOperations, context))
		return true;
	if (walker(query->havingQual, context))
		return true;
	if (walker(query->limitOffset, context))
		return true;
	if (walker(query->limitCount, context))
		return true;

	/*
	 * Most callers aren't interested in SortGroupClause nodes since those
	 * don't contain actual expressions. However they do contain OIDs which
	 * may be needed by dependency walkers etc.
	 */
	if ((flags & QTW_EXAMINE_SORTGROUP))
	{
		if (walker((PGNode *) query->groupClause, context))
			return true;
		if (walker((PGNode *) query->windowClause, context))
			return true;
		if (walker((PGNode *) query->sortClause, context))
			return true;
		if (walker((PGNode *) query->distinctClause, context))
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

			if (walker(wc->startOffset, context))
				return true;
			if (walker(wc->endOffset, context))
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
		if (walker((PGNode *) query->cteList, context))
			return true;
	}
	if (!(flags & QTW_IGNORE_RANGE_TABLE))
	{
		if (range_table_walker(query->rtable, walker, context, flags))
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
			if (walker(((PGCopyStmt *) query->utilityStmt)->query, context))
				return true;
		}
		// if (IsA(query->utilityStmt, DeclareCursorStmt))
		// {
		// 	if (walker(((DeclareCursorStmt *) query->utilityStmt)->query, context))
		// 		return true;
		// }
		if (IsA(query->utilityStmt, PGExplainStmt))
		{
			if (walker(((PGExplainStmt *) query->utilityStmt)->query, context))
				return true;
		}
		if (IsA(query->utilityStmt, PGPrepareStmt))
		{
			if (walker(((PGPrepareStmt *) query->utilityStmt)->query, context))
				return true;
		}
		if (IsA(query->utilityStmt, PGViewStmt))
		{
			if (walker(((PGViewStmt *) query->utilityStmt)->query, context))
				return true;
		}
	}
	return false;
};

bool
contain_aggs_of_level_walker(duckdb_libpgquery::PGNode *node,
							 contain_aggs_of_level_context *context)
{
    using duckdb_libpgquery::PGAggref;
    using duckdb_libpgquery::PGQuery;
    using duckdb_libpgquery::PGNode;
    using duckdb_libpgquery::T_PGAggref;
    using duckdb_libpgquery::T_PGQuery;

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
		result = query_tree_walker((PGQuery *) node,
								   contain_aggs_of_level_walker,
								   (void *) context, 0);
		context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node, contain_aggs_of_level_walker,
								  (void *) context);
};

bool
query_or_expression_tree_walker(duckdb_libpgquery::PGNode *node,
								walker_func walker,
								void *context,
								int flags)
{
	using duckdb_libpgquery::PGQuery;
	if (node && IsA(node, PGQuery))
		return query_tree_walker((PGQuery *) node,
								 walker,
								 context,
								 flags);
	else
		return walker(node, context);
};

bool
contain_aggs_of_level(duckdb_libpgquery::PGNode *node, int levelsup)
{
	contain_aggs_of_level_context context;

	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	return query_or_expression_tree_walker(node,
										   contain_aggs_of_level_walker,
										   (void *) &context,
										   0);
};

bool
contain_windowfuncs_walker(duckdb_libpgquery::PGNode *node, void *context)
{
	using duckdb_libpgquery::PGWindowFunc;
	using duckdb_libpgquery::PGNode;
	if (node == NULL)
		return false;
	if (IsA(node, PGWindowFunc))
		return true;			/* abort the tree traversal and return true */
	/* Mustn't recurse into subselects */
	return expression_tree_walker(node, contain_windowfuncs_walker,
								  (void *) context);
};

bool
contain_windowfuncs(duckdb_libpgquery::PGNode *node)
{
	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	return query_or_expression_tree_walker(node,
										   contain_windowfuncs_walker,
										   NULL,
										   0);
};

bool
query_or_expression_tree_walker(duckdb_libpgquery::PGNode *node,
								bool (*walker) (),
								void *context,
								int flags)
{
	using duckdb_libpgquery::PGQuery;
	if (node && IsA(node, PGQuery))
		return query_tree_walker((PGQuery *) node,
								 walker,
								 context,
								 flags);
	else
		return walker(node, context);
};
