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
        expr = expr_parser.transformExpr(pstate, node);

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
                aliasvar = (PGVar *)list_nth(rte->joinaliasvars, attnum - 1);
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

PGList * TargetParser::ExpandAllTables(PGParseState * pstate)
{
    PGList * target = NIL;
    PGListCell * l;

    /* Check for SELECT *; */
    if (!pstate->p_varnamespace)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("SELECT * with no tables specified is not valid")));

    foreach (l, pstate->p_varnamespace)
    {
        PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(l);
        int rtindex = relation_parser.RTERangeTablePosn(pstate, rte, NULL);

        /* Require read access --- see comments in setTargetTable() */
        rte->requiredPerms |= ACL_SELECT;

        target = list_concat(target, relation_parser.expandRelAttrs(pstate, rte, rtindex, 0, -1));
    }

    return target;
};

PGList * TargetParser::ExpandColumnRefStar(PGParseState * pstate, PGColumnRef * cref, bool targetlist)
{
    PGList * fields = cref->fields;
    int numnames = list_length(fields);

    if (numnames == 1)
    {
        /*
		 * Target item is a bare '*', expand all tables
		 *
		 * (e.g., SELECT * FROM emp, dept)
		 *
		 * Since the grammar only accepts bare '*' at top level of SELECT, we
		 * need not handle the targetlist==false case here.  However, we must
		 * test for it because the grammar currently fails to distinguish
		 * a quoted name "*" from a real asterisk.
		 */
        if (!targetlist)
            elog(ERROR, "invalid use of *");

        return ExpandAllTables(pstate);
    }
    else
    {
        /*
		 * Target item is relation.*, expand that table
		 *
		 * (e.g., SELECT emp.*, dname FROM emp, dept)
		 */
        char * catalogname = NULL;
        char * schemaname = NULL;
        char * relname = NULL;
        PGRangeTblEntry * rte;
        int sublevels_up;
        int rtindex;

        switch (numnames)
        {
            case 2:
                relname = strVal(linitial(fields));
                break;
            case 3:
                schemaname = strVal(linitial(fields));
                relname = strVal(lsecond(fields));
                break;
            case 4: {
                catalogname = strVal(linitial(fields));
                schemaname = strVal(lsecond(fields));
                relname = strVal(lthird(fields));
                break;
            }
            default:
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("improper qualified name (too many dotted names): %s", NameListToString(fields)),
                     parser_errposition(pstate, cref->location)));
                schemaname = NULL; /* keep compiler quiet */
                relname = NULL;
                break;
        }

        rte = relation_parser.refnameRangeTblEntry(pstate, catalogname, schemaname, relname, cref->location, &sublevels_up);
        if (rte == NULL)
        {
            rte = relation_parser.addImplicitRTE(pstate, makeRangeVar(catalogname, schemaname, relname, cref->location), cref->location);
        }

        /* Require read access --- see comments in setTargetTable() */
        rte->requiredPerms |= ACL_SELECT;

        rtindex = relation_parser.RTERangeTablePosn(pstate, rte, &sublevels_up);

        if (targetlist)
            return relation_parser.expandRelAttrs(pstate, rte, rtindex, sublevels_up, cref->location);
        else
        {
            PGList * vars;

            relation_parser.expandRTE(rte, rtindex, sublevels_up, cref->location, false, NULL, &vars);
            return vars;
        }
    }
};

TupleDesc TargetParser::expandRecordVariable(PGParseState * pstate, PGVar * var, int levelsup)
{
    TupleDesc tupleDesc;
    int netlevelsup;
    PGRangeTblEntry * rte;
    PGAttrNumber attnum;
    PGNode * expr;

    /* Check my caller didn't mess up */
    Assert(IsA(var, PGVar));
    Assert(var->vartype == RECORDOID);

    netlevelsup = var->varlevelsup + levelsup;
    rte = relation_parser.GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
    attnum = var->varattno;

    if (attnum == InvalidAttrNumber)
    {
        /* Whole-row reference to an RTE, so expand the known fields */
        PGList *names, *vars;
        PGListCell *lname, *lvar;
        int i;

        relation_parser.expandRTE(rte, var->varno, 0, -1, false, &names, &vars);

        tupleDesc = CreateTemplateTupleDesc(list_length(vars), false);
        i = 1;
        forboth(lname, names, lvar, vars)
        {
            char * label = strVal(lfirst(lname));
            PGNode * varnode = (PGNode *)lfirst(lvar);

            TupleDescInitEntry(tupleDesc, (PGAttrNumber)i, label, exprType(varnode), exprTypmod(varnode), 0);
            i++;
        }
        Assert(lname == NULL && lvar == NULL); /* lists same length? */

        return tupleDesc;
    }

    expr = (PGNode *)var; /* default if we can't drill down */

    switch (rte->rtekind)
    {
        case PG_RTE_RELATION:
        // case RTE_SPECIAL:
        case PG_RTE_VALUES:

            /*
			 * This case should not occur: a column of a table or values list
			 * shouldn't have type RECORD.  Fall through and fail (most
			 * likely) at the bottom.
			 */
            break;
        case PG_RTE_SUBQUERY: {
            /* Subselect-in-FROM: examine sub-select's output expr */
            PGTargetEntry * ste = relation_parser.get_tle_by_resno(rte->subquery->targetList, attnum);

            if (ste == NULL || ste->resjunk)
                elog(ERROR, "subquery %s does not have attribute %d", rte->eref->aliasname, attnum);
            expr = (PGNode *)ste->expr;
            if (IsA(expr, PGVar))
            {
                /*
					 * Recurse into the sub-select to see what its Var refers
					 * to.	We have to build an additional level of ParseState
					 * to keep in step with varlevelsup in the subselect.
					 */
                PGParseState mypstate;

                MemSet(&mypstate, 0, sizeof(mypstate));
                mypstate.parentParseState = pstate;
                mypstate.p_rtable = rte->subquery->rtable;
                /* don't bother filling the rest of the fake pstate */

                return expandRecordVariable(&mypstate, (PGVar *)expr, 0);
            }
            /* else fall through to inspect the expression */
        }
        break;
        case PG_RTE_CTE:
            if (!rte->self_reference)
            {
                /* Similar to RTE_SUBQUERY */
                PGCommonTableExpr * cte = cte_parser.GetCTEForRTE(pstate, rte, netlevelsup);
                Assert(cte != NULL);

                PGTargetEntry * ste = relation_parser.get_tle_by_resno(GetCTETargetList(cte), attnum);
                if (ste == NULL || ste->resjunk)
                    elog(ERROR, "WITH query %s does not have attribute %d", cte->ctename, attnum);

                expr = (PGNode *)ste->expr;
                if (IsA(expr, PGVar))
                {
                    /*
					 * Recurse into the sub-select to see what its Var refers
					 * to.	We have to build an additional level of ParseState
					 * to keep in step with varlevelsup in the subselect.
					 */
                    PGParseState mypstate;

                    MemSet(&mypstate, 0, sizeof(mypstate));

                    for (Index levelsup = 0; levelsup < rte->ctelevelsup + netlevelsup; levelsup++)
                        pstate = pstate->parentParseState;

                    mypstate.parentParseState = pstate;
                    mypstate.p_rtable = ((PGQuery *)cte->ctequery)->rtable;
                    /* don't bother filling the rest of the fake pstate */

                    return expandRecordVariable(&mypstate, (PGVar *)expr, 0);
                }
                /* else fall through to inspect the expression */
            }
            break;
        case PG_RTE_JOIN:
            /* Join RTE --- recursively inspect the alias variable */
            Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
            expr = (PGNode *)list_nth(rte->joinaliasvars, attnum - 1);
            if (IsA(expr, PGVar))
                return expandRecordVariable(pstate, (PGVar *)expr, netlevelsup);
            /* else fall through to inspect the expression */
            break;
        // case RTE_TABLEFUNCTION:
        case PG_RTE_FUNCTION:

            /*
			 * We couldn't get here unless a function is declared with one of
			 * its result columns as RECORD, which is not allowed.
			 */
            break;
        case RTE_VOID:
            Insist(0);
            break;
    }

    /*
	 * We now have an expression we can't expand any more, so see if
	 * get_expr_result_type() can do anything with it.	If not, pass to
	 * lookup_rowtype_tupdesc() which will probably fail, but will give an
	 * appropriate error message while failing.
	 */
    if (get_expr_result_type(expr, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        tupleDesc = lookup_rowtype_tupdesc_copy(exprType(expr), exprTypmod(expr));

    return tupleDesc;
};

PGList * TargetParser::ExpandIndirectionStar(PGParseState * pstate, PGAIndirection * ind, bool targetlist)
{
    PGList * result = NIL;
    PGNode * expr;
    TupleDesc tupleDesc;
    int numAttrs;
    int i;

    /* Strip off the '*' to create a reference to the rowtype object */
    ind = copyObject(ind);
    ind->indirection = list_truncate(ind->indirection, list_length(ind->indirection) - 1);

    /* And transform that */
    expr = transformExpr(pstate, (PGNode *)ind);

    /*
	 * Verify it's a composite type, and get the tupdesc.  We use
	 * get_expr_result_type() because that can handle references to functions
	 * returning anonymous record types.  If that fails, use
	 * lookup_rowtype_tupdesc(), which will almost certainly fail as well, but
	 * it will give an appropriate error message.
	 *
	 * If it's a Var of type RECORD, we have to work even harder: we have to
	 * find what the Var refers to, and pass that to get_expr_result_type.
	 * That task is handled by expandRecordVariable().
	 */
    if (IsA(expr, PGVar) && ((PGVar *)expr)->vartype == RECORDOID)
        tupleDesc = expandRecordVariable(pstate, (PGVar *)expr, 0);
    else if (get_expr_result_type(expr, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        tupleDesc = lookup_rowtype_tupdesc_copy(exprType(expr), exprTypmod(expr));
    Assert(tupleDesc);

    /* Generate a list of references to the individual fields */
    numAttrs = tupleDesc->natts;
    for (i = 0; i < numAttrs; i++)
    {
        Form_pg_attribute att = tupleDesc->attrs[i];
        PGNode * fieldnode;

        if (att->attisdropped)
            continue;

        /*
		 * If we got a whole-row Var from the rowtype reference, we can expand
		 * the fields as simple Vars.  Otherwise we must generate multiple
		 * copies of the rowtype reference and do FieldSelects.
		 */
        if (IsA(expr, PGVar) && ((PGVar *)expr)->varattno == InvalidAttrNumber)
        {
            PGVar * var = (PGVar *)expr;

            fieldnode = (PGNode *)makeVar(var->varno, i + 1, att->atttypid, att->atttypmod, var->varlevelsup);
        }
        else
        {
            PGFieldSelect * fselect = makeNode(PGFieldSelect);

            fselect->arg = (PGExpr *)copyObject(expr);
            fselect->fieldnum = i + 1;
            fselect->resulttype = att->atttypid;
            fselect->resulttypmod = att->atttypmod;

            fieldnode = (PGNode *)fselect;
        }

        if (targetlist)
        {
            /* add TargetEntry decoration */
            PGTargetEntry * te;

            te = makeTargetEntry((PGExpr *)fieldnode, (PGAttrNumber)pstate->p_next_resno++, pstrdup(NameStr(att->attname)), false);
            result = lappend(result, te);
        }
        else
            result = lappend(result, fieldnode);
    }

    return result;
};

PGList * TargetParser::transformExpressionList(PGParseState * pstate, PGList * exprlist)
{
    PGList * result = NIL;
    PGListCell * lc;
    ParseStateBreadCrumb savebreadcrumb;

    /* CDB: Push error location stack.  Must pop before return! */
    Assert(pstate);
    savebreadcrumb = pstate->p_breadcrumb;
    pstate->p_breadcrumb.pop = &savebreadcrumb;

    foreach (lc, exprlist)
    {
        PGNode * e = (PGNode *)lfirst(lc);

        /* CDB: Drop a breadcrumb in case of error. */
        pstate->p_breadcrumb.node = (PGNode *)e;

        /*
		 * Check for "something.*".  Depending on the complexity of the
		 * "something", the star could appear as the last name in ColumnRef,
		 * or as the last indirection item in A_Indirection.
		 */
        if (IsA(e, PGColumnRef))
        {
            PGColumnRef * cref = (PGColumnRef *)e;

            if (strcmp(strVal(llast(cref->fields)), "*") == 0)
            {
                /* It is something.*, expand into multiple items */
                result = list_concat(result, ExpandColumnRefStar(pstate, cref, false));
                continue;
            }
        }
        else if (IsA(e, PGAIndirection))
        {
            PGAIndirection * ind = (PGAIndirection *)e;
            PGNode * lastitem = llast(ind->indirection);

            if (IsA(lastitem, String) && strcmp(strVal(lastitem), "*") == 0)
            {
                /* It is something.*, expand into multiple items */
                result = list_concat(result, ExpandIndirectionStar(pstate, ind, false));
                continue;
            }
        }

        /*
		 * Not "something.*", so transform as a single expression
		 */
        result = lappend(result, expr_parser.transformExpr(pstate, e));
    }

    /* CDB: Pop error location stack. */
    Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
    pstate->p_breadcrumb = savebreadcrumb;

    return result;
};

}