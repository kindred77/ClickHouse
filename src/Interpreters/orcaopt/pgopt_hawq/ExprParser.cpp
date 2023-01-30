#include <Interpreters/orcaopt/pgopt_hawq/ExprParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGNode * ExprParser::transformExpr(PGParseState * pstate,
        PGNode * expr)
{
    PGNode * result;
    ParseStateBreadCrumb savebreadcrumb;

    if (expr == NULL)
        return NULL;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    /* CDB: Drop a breadcrumb, then push location stack. Must pop before return! */
    Assert(pstate);
    pstate->p_breadcrumb.node = (PGNode *)expr;
    savebreadcrumb = pstate->p_breadcrumb;
    pstate->p_breadcrumb.pop = &savebreadcrumb;

    result = NULL;
    switch (nodeTag(expr))
    {
        case T_PGColumnRef:
            result = transformColumnRef(pstate, (PGColumnRef *)expr);
            break;

        case T_PGParamRef:
            result = transformParamRef(pstate, (PGParamRef *)expr);
            break;

        case T_PGAConst: {
            PGAConst * con = (PGAConst *)expr;
            PGValue * val = &con->val;

            result = (PGNode *)make_const(pstate, val, -1);
            if (con->typname != NULL)
                result = typecast_expression(pstate, result, con->typname);
            break;
        }

        case T_PGAIndirection: {
            PGAIndirection * ind = (PGAIndirection *)expr;

            result = transformExpr(pstate, ind->arg);
            result = transformIndirection(pstate, result, ind->indirection);
            break;
        }

        case T_PGTypeCast: {
            PGTypeCast * tc = (PGTypeCast *)expr;
            PGNode * arg = transformExpr(pstate, tc->arg);

            result = typecast_expression(pstate, arg, tc->typname);
            break;
        }

        case T_PGAExpr: {
            PGAExpr * a = (PGAExpr *)expr;

            switch (a->kind)
            {
                case PG_AEXPR_OP:
                    result = transformAExprOp(pstate, a);
                    break;
                case AEXPR_AND:
                    result = transformAExprAnd(pstate, a);
                    break;
                case AEXPR_OR:
                    result = transformAExprOr(pstate, a);
                    break;
                case AEXPR_NOT:
                    result = transformAExprNot(pstate, a);
                    break;
                case PG_AEXPR_OP_ANY:
                    result = transformAExprOpAny(pstate, a);
                    break;
                case PG_AEXPR_OP_ALL:
                    result = transformAExprOpAll(pstate, a);
                    break;
                case PG_AEXPR_DISTINCT:
                    result = transformAExprDistinct(pstate, a);
                    break;
                case PG_AEXPR_NULLIF:
                    result = transformAExprNullIf(pstate, a);
                    break;
                case PG_AEXPR_OF:
                    result = transformAExprOf(pstate, a);
                    break;
                case PG_AEXPR_IN:
                    result = transformAExprIn(pstate, a);
                    break;
                default:
                    elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
            }
            break;
        }

        case T_PGFuncCall:
            result = transformFuncCall(pstate, (PGFuncCall *)expr);
            break;


        case T_PGSubLink:
            result = transformSubLink(pstate, (PGSubLink *)expr);
            break;

        case T_PGCaseExpr:
            result = transformCaseExpr(pstate, (PGCaseExpr *)expr);
            break;

        case T_PGArrayExpr:
            result = transformArrayExpr(pstate, (PGArrayExpr *)expr);
            break;

        case T_PGRowExpr:
            result = transformRowExpr(pstate, (PGRowExpr *)expr);
            break;

        // case T_TableValueExpr:
        //     result = transformTableValueExpr(pstate, (TableValueExpr *)expr);
        //     break;

        case T_PGCoalesceExpr:
            result = transformCoalesceExpr(pstate, (PGCoalesceExpr *)expr);
            break;

        case T_PGMinMaxExpr:
            result = transformMinMaxExpr(pstate, (PGMinMaxExpr *)expr);
            break;

        case T_PGNullTest: {
            PGNullTest * n = (PGNullTest *)expr;

            n->arg = (PGExpr *)transformExpr(pstate, (PGNode *)n->arg);
            /* the argument can be any type, so don't coerce it */
            result = expr;
            break;
        }

        case T_PGBooleanTest:
            result = transformBooleanTest(pstate, (PGBooleanTest *)expr);
            break;

        case T_PGCurrentOfExpr: {
            /*
				 * The target RTE must be simply updatable. If not, we error out
				 * early here to avoid having to deal with error cases later:
				 * rewriting/planning against views, for example.
				 */
            Assert(pstate->p_target_rangetblentry != NULL);
            if (!isSimplyUpdatableRelation(pstate->p_target_rangetblentry->relid))
                ereport(
                    ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("\"%s\" is not simply updatable", pstate->p_target_relation->rd_rel->relname.data)));

            PGCurrentOfExpr * c = (PGCurrentOfExpr *)expr;
            int sublevels_up;
            c->cvarno = RTERangeTablePosn(pstate, pstate->p_target_rangetblentry, &sublevels_up);
            c->target_relid = pstate->p_target_rangetblentry->relid;
            Assert(sublevels_up == 0);
            result = expr;
            break;
        }

        case T_PGGroupingFunc: {
            PGGroupingFunc * gf = (PGGroupingFunc *)expr;
            result = transformGroupingFunc(pstate, gf);
            break;
        }

        case T_PGPartitionBoundSpec: {
            PartitionBoundSpec * in = (PartitionBoundSpec *)expr;
            PartitionRangeItem * ri;
            PGList * out = NIL;
            PGListCell * lc;

            if (in->partStart)
            {
                ri = (PartitionRangeItem *)in->partStart;

                /* ALTER TABLE ... ADD PARTITION might feed
					 * "pre-cooked" expressions into the boundspec for
					 * range items (which are Lists) 
					 */
                {
                    Assert(IsA(in->partStart, PartitionRangeItem));

                    foreach (lc, ri->partRangeVal)
                    {
                        PGNode * n = lfirst(lc);
                        out = lappend(out, transformExpr(pstate, n));
                    }
                    ri->partRangeVal = out;
                    out = NIL;
                }
            }
            if (in->partEnd)
            {
                ri = (PartitionRangeItem *)in->partEnd;

                /* ALTER TABLE ... ADD PARTITION might feed
					 * "pre-cooked" expressions into the boundspec for
					 * range items (which are Lists) 
					 */
                {
                    Assert(IsA(in->partEnd, PartitionRangeItem));
                    foreach (lc, ri->partRangeVal)
                    {
                        PGNode * n = lfirst(lc);
                        out = lappend(out, transformExpr(pstate, n));
                    }
                    ri->partRangeVal = out;
                    out = NIL;
                }
            }
            if (in->partEvery)
            {
                ri = (PartitionRangeItem *)in->partEvery;
                Assert(IsA(in->partEvery, PartitionRangeItem));
                foreach (lc, ri->partRangeVal)
                {
                    PGNode * n = lfirst(lc);
                    out = lappend(out, transformExpr(pstate, n));
                }
                ri->partRangeVal = out;
            }

            result = (PGNode *)in;
        }
        break;

        case T_PercentileExpr:
            result = transformPercentileExpr(pstate, (PercentileExpr *)expr);
            break;

            /*********************************************
			 * Quietly accept node types that may be presented when we are
			 * called on an already-transformed tree.
			 *
			 * Do any other node types need to be accepted?  For now we are
			 * taking a conservative approach, and only accepting node
			 * types that are demonstrably necessary to accept.
			 *********************************************/
        case T_PGVar:
        case T_PGConst:
        case T_PGParam:
        case T_PGAggref:
        case T_PGArrayRef:
        case T_PGFuncExpr:
        case T_PGOpExpr:
        case T_PGDistinctExpr:
        case T_PGScalarArrayOpExpr:
        case T_PGNullIfExpr:
        case T_PGBoolExpr:
        case T_PGFieldSelect:
        case T_PGFieldStore:
        case T_PGRelabelType:
        case T_PGConvertRowtypeExpr:
        case T_PGCaseTestExpr:
        case T_PGCoerceToDomain:
        case T_PGCoerceToDomainValue:
        case T_PGSetToDefault:
        // case T_GroupId:
        case T_PGInteger: {
            result = (PGNode *)expr;
            break;
        }

        default:
            /* should not reach here */
            elog(ERROR, "unrecognized node type: %d", (int)nodeTag(expr));
            break;
    }

    /* CDB: Pop error location stack, leaving breadcrumb on our input expr. */
    Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
    pstate->p_breadcrumb = savebreadcrumb;

    return result;
};

}