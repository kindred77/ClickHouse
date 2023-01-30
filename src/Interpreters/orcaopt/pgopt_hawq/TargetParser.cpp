#include <Interpreters/orcaopt/pgopt_hawq/TargetParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

int TargetParser::FigureColnameInternal(PGNode * node, char ** name)
{
    int strength = 0;

    if (node == NULL)
        return strength;

    switch (nodeTag(node))
    {
        case T_PGColumnRef: {
            char * fname = NULL;
            PGListCell * l;

            /* find last field name, if any, ignoring "*" */
            foreach (l, ((PGColumnRef *)node)->fields)
            {
                PGNode * i = lfirst(l);

                if (strcmp(strVal(i), "*") != 0)
                    fname = strVal(i);
            }
            if (fname)
            {
                *name = fname;
                return 2;
            }
        }
        break;
        case T_PGAIndirection: {
            PGAIndirection * ind = (PGAIndirection *)node;
            char * fname = NULL;
            PGListCell * l;

            /* find last field name, if any, ignoring "*" */
            foreach (l, ind->indirection)
            {
                PGNode * i = lfirst(l);

                if (IsA(i, PGString) && strcmp(strVal(i), "*") != 0)
                    fname = strVal(i);
            }
            if (fname)
            {
                *name = fname;
                return 2;
            }
            return FigureColnameInternal(ind->arg, name);
        }
        break;
        case T_PGFuncCall:
            *name = strVal(llast(((FuncCall *)node)->funcname));
            return 2;
        case T_PGAExpr:
            /* make nullif() act like a regular function */
            if (((PGAExpr *)node)->kind == PG_AEXPR_NULLIF)
            {
                *name = "nullif";
                return 2;
            }
            break;
        case T_PGAConst:
            if (((PGAConst *)node)->typname != NULL)
            {
                *name = strVal(llast(((PGAConst *)node)->typname->names));
                return 1;
            }
            break;
        case T_PGTypeCast:
            strength = FigureColnameInternal(((PGTypeCast *)node)->arg, name);
            if (strength <= 1)
            {
                if (((PGTypeCast *)node)->typname != NULL)
                {
                    *name = strVal(llast(((PGTypeCast *)node)->typname->names));
                    return 1;
                }
            }
            break;
        case T_PGCaseExpr:
            strength = FigureColnameInternal((PGNode *)((PGCaseExpr *)node)->defresult, name);
            if (strength <= 1)
            {
                *name = "case";
                return 1;
            }
            break;
        case T_PGArrayExpr:
            /* make ARRAY[] act like a function */
            *name = "array";
            return 2;
        case T_PGRowExpr:
            /* make ROW() act like a function */
            *name = "row";
            return 2;
        case T_PGCoalesceExpr:
            /* make coalesce() act like a regular function */
            *name = "coalesce";
            return 2;
        case T_PGMinMaxExpr:
            /* make greatest/least act like a regular function */
            switch (((PGMinMaxExpr *)node)->op)
            {
                case PG_IS_GREATEST:
                    *name = "greatest";
                    return 2;
                case IS_LEAST:
                    *name = "least";
                    return 2;
            }
            break;
        case T_PGGroupingFunc:
            *name = "grouping";
            return 2;
        // case T_PercentileExpr:
        //     switch (((PercentileExpr *)node)->perckind)
        //     {
        //         case PERC_MEDIAN:
        //             *name = "median";
        //             break;
        //         case PERC_CONT:
        //             *name = "percentile_cont";
        //             break;
        //         case PERC_DISC:
        //             *name = "percentile_disc";
        //             break;
        //         default:
        //             elog(ERROR, "unexpected percentile type");
        //             break;
        //     }
        //     return 2;
        default:
            break;
    }

    return strength;
};

char * TargetParser::FigureColname(PGNode *node)
{
    char * name = NULL;

    FigureColnameInternal(node, &name);
    if (name != NULL)
        return name;
    /* default result if we can't guess anything */
    return "?column?";
};

PGTargetEntry * TargetParser::transformTargetEntry(PGParseState * pstate,
        PGNode * node, PGNode * expr,
        char * colname, bool resjunk)
{
    /* Transform the node if caller didn't do it already */
    if (expr == NULL)
        expr = transformExpr(pstate, node);

    if (colname == NULL && !resjunk)
    {
        /*
		 * Generate a suitable column name for a column without any explicit
		 * 'AS ColumnName' clause.
		 */
        colname = FigureColname(node);
    }

    return makeTargetEntry((PGExpr *)expr, (PGAttrNumber)pstate->p_next_resno++, colname, resjunk);
};

PGList * TargetParser::transformTargetList(PGParseState * pstate,
        PGList * targetlist)
{
    PGList * p_target = NIL;
    PGListCell * o_target;
    ParseStateBreadCrumb savebreadcrumb;

    /* CDB: Push error location stack.  Must pop before return! */
    Assert(pstate);
    savebreadcrumb = pstate->p_breadcrumb;
    pstate->p_breadcrumb.pop = &savebreadcrumb;

    foreach (o_target, targetlist)
    {
        PGResTarget * res = (PGResTarget *)lfirst(o_target);

        /* CDB: Drop a breadcrumb in case of error. */
        pstate->p_breadcrumb.node = (PGNode *)res;

        /*
		 * Check for "something.*".  Depending on the complexity of the
		 * "something", the star could appear as the last name in ColumnRef,
		 * or as the last indirection item in A_Indirection.
		 */
        if (IsA(res->val, PGColumnRef))
        {
            PGColumnRef * cref = (PGColumnRef *)res->val;

            if (strcmp(strVal(llast(cref->fields)), "*") == 0)
            {
                /* It is something.*, expand into multiple items */
                p_target = list_concat(p_target, ExpandColumnRefStar(pstate, cref, true));
                continue;
            }
        }
        else if (IsA(res->val, PGAIndirection))
        {
            PGAIndirection * ind = (PGAIndirection *)res->val;
            PGNode * lastitem = llast(ind->indirection);

            if (IsA(lastitem, PGString) && strcmp(strVal(lastitem), "*") == 0)
            {
                /* It is something.*, expand into multiple items */
                p_target = list_concat(p_target, ExpandIndirectionStar(pstate, ind, true));
                continue;
            }
        }

        /*
		 * Not "something.*", so transform as a single expression
		 */
        p_target = lappend(p_target, transformTargetEntry(pstate, res->val, NULL, res->name, false));
    }

    /* CDB: Pop error location stack. */
    Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
    pstate->p_breadcrumb = savebreadcrumb;

    return p_target;
};

void TargetParser::markTargetListOrigin(PGParseState *pstate, PGTargetEntry *tle,
					 PGVar *var, int levelsup)
{
    int netlevelsup;
    PGRangeTblEntry * rte;
    PGAttrNumber attnum;

    if (var == NULL || !IsA(var, PGVar))
        return;
    netlevelsup = var->varlevelsup + levelsup;
    rte = relation_parser.GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
    attnum = var->varattno;

    switch (rte->rtekind)
    {
        case PG_RTE_RELATION:
            /* It's a table or view, report it */
            tle->resorigtbl = rte->relid;
            tle->resorigcol = attnum;
            break;
        case PG_RTE_SUBQUERY:
            /* Subselect-in-FROM: copy up from the subselect */
            if (attnum != InvalidAttrNumber)
            {
                PGTargetEntry * ste = relation_parser.get_tle_by_resno(rte->subquery->targetList, attnum);

                if (ste == NULL || ste->resjunk)
                    elog(ERROR, "subquery %s does not have attribute %d", rte->eref->aliasname, attnum);
                tle->resorigtbl = ste->resorigtbl;
                tle->resorigcol = ste->resorigcol;
            }
            break;
        case PG_RTE_CTE:
            /* Similar to RTE_SUBQUERY */
            if (attnum != InvalidAttrNumber)
            {
                /* Find the CommonTableExpr based on the query name */
                PGCommonTableExpr * cte = cte_parser.GetCTEForRTE(pstate, rte, netlevelsup);
                Assert(cte != NULL);

                PGTargetEntry * ste = relation_parser.get_tle_by_resno(GetCTETargetList(cte), attnum);
                if (ste == NULL || ste->resjunk)
                {
                    elog(ERROR, "WITH query %s does not have attribute %d", rte->ctename, attnum);
                }

                tle->resorigtbl = ste->resorigtbl;
                tle->resorigcol = ste->resorigcol;
            }
            break;
        case PG_RTE_JOIN:
            /* Join RTE --- recursively inspect the alias variable */
            if (attnum != InvalidAttrNumber)
            {
                PGVar * aliasvar;

                Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
                aliasvar = (Var *)list_nth(rte->joinaliasvars, attnum - 1);
                markTargetListOrigin(pstate, tle, aliasvar, netlevelsup);
            }
            break;
        // case RTE_SPECIAL:
        // case RTE_TABLEFUNCTION:
        case PG_RTE_FUNCTION:
        case PG_RTE_VALUES:
        // case RTE_VOID:
            /* not a simple relation, leave it unmarked */
            break;
    }
};

void TargetParser::markTargetListOrigins(PGParseState *pstate, PGList *targetlist)
{
    PGListCell * l;

    foreach (l, targetlist)
    {
        PGTargetEntry * tle = (PGTargetEntry *)lfirst(l);

        markTargetListOrigin(pstate, tle, (PGVar *)tle->expr, 0);
    }
};

}