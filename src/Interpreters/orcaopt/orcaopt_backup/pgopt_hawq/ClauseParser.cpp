#include <Interpreters/orcaopt/ClauseParser.h>

#include <Interpreters/orcaopt/walkers.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/ExprParser.h>
#include <Interpreters/orcaopt/TargetParser.h>
#include <Interpreters/orcaopt/OperParser.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

bool pg_grouping_rewrite_walker(PGNode * node, void * context)
{
    // using duckdb_libpgquery::PGListCell;
    // using duckdb_libpgquery::PGList;
    // using duckdb_libpgquery::PGNode;
    // using duckdb_libpgquery::PGTargetEntry;
    // using duckdb_libpgquery::PGRangeTblEntry;
    // using duckdb_libpgquery::PGVar;
    // using duckdb_libpgquery::PGRowExpr;

    // using duckdb_libpgquery::PGAExpr;
    // using duckdb_libpgquery::PGColumnRef;
    // using duckdb_libpgquery::PGAConst;
    // using duckdb_libpgquery::PGTypeCast;
    // using duckdb_libpgquery::PGGroupingFunc;
    // using duckdb_libpgquery::PGSortBy;
    // using duckdb_libpgquery::T_PGAExpr;
    // using duckdb_libpgquery::T_PGColumnRef;
    // using duckdb_libpgquery::T_PGAConst;
    // using duckdb_libpgquery::T_PGTypeCast;
    // using duckdb_libpgquery::T_PGGroupingFunc;
    // using duckdb_libpgquery::T_PGVar;
    // using duckdb_libpgquery::T_PGRowExpr;
    // using duckdb_libpgquery::T_PGSortBy;

    // using duckdb_libpgquery::ereport;
    // using duckdb_libpgquery::errcode;
    // using duckdb_libpgquery::errmsg;
    // using duckdb_libpgquery::makeInteger;

    pg_grouping_rewrite_ctx * ctx = (pg_grouping_rewrite_ctx *)context;

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

        //gf->ngrpcols = list_length(ctx->grp_tles);

        /*
		 * For each argument in gf->args, find its position in grp_tles,
		 * and increment its counts. Note that this is a O(n^2) algorithm,
		 * but it should not matter that much.
		 */
        foreach (arg_lc, gf->args)
        {
            long i = 0;
            PGNode * node_arg = (PGNode *)lfirst(arg_lc);
            PGListCell * grp_lc = NULL;

            foreach (grp_lc, ctx->grp_tles)
            {
                PGTargetEntry * grp_tle = (PGTargetEntry *)lfirst(grp_lc);

                if (equal(grp_tle->expr, node_arg))
                    break;
                i++;
            }

            /* Find a column not in GROUP BY clause */
            if (grp_lc == NULL)
            {
                PGRangeTblEntry * rte;
                const char * attname;
                PGVar * var = (PGVar *)node_arg;

                /* Do not allow expressions inside a grouping function. */
                if (IsA(node_arg, PGRowExpr))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_GROUPING_ERROR),
                         errmsg("row type can not be used inside a grouping function.")/* ,
                         errOmitLocation(true) */));

                if (!IsA(node_arg, PGVar))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_GROUPING_ERROR),
                         errmsg("expression in a grouping fuction does not appear in GROUP BY.")/* ,
                         errOmitLocation(true) */));

                Assert(IsA(node_arg, PGVar))
                Assert(var->varno > 0)
                Assert(var->varno <= list_length(ctx->pstate->p_rtable))

                rte = rt_fetch(var->varno, ctx->pstate->p_rtable);
                attname = RelationParser::get_rte_attribute_name(rte, var->varattno);

                ereport(
                    ERROR,
                    (errcode(ERRCODE_GROUPING_ERROR),
                     errmsg("column \"%s\".\"%s\" is not in GROUP BY", rte->eref->aliasname, attname)/* ,
                     errOmitLocation(true) */));
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
		 * When WindowSpec leaves the main parser, partition and order
		 * clauses will be lists of SortBy structures. Process them here to
		 * avoid muddying up the expression_tree_walker().
		 */
        PGSortBy * s = (PGSortBy *)node;
        return pg_grouping_rewrite_walker(s->node, context);
    }
    return pg_expression_tree_walker(node, (walker_func)pg_grouping_rewrite_walker, context);
};

bool pull_varnos_cbVar(PGVar * var, void * context, int sublevelsup)
{
    pull_varnos_context * ctx = (pull_varnos_context *)context;

    if ((int)var->varlevelsup == sublevelsup)
        ctx->varnos = bms_add_member(ctx->varnos, var->varno);
    return false;
};

bool pull_varnos_cbCurrentOf(PGCurrentOfExpr * expr, void * context, int sublevelsup)
{
    Assert(sublevelsup == 0)
    pull_varnos_context * ctx = (pull_varnos_context *)context;
    ctx->varnos = bms_add_member(ctx->varnos, expr->cvarno);
    return false;
};

PGRelids pull_varnos_of_level(PGNode * node, int levelsup) /*CDB*/
{
    pull_varnos_context context;

    context.varnos = NULL;
    cdb_walk_vars(node, pull_varnos_cbVar, NULL, pull_varnos_cbCurrentOf, &context, levelsup);
    return context.varnos;
}; /* pull_varnos_of_level */

PGRelids pull_varnos(PGNode * node)
{
    return pull_varnos_of_level(node, 0);
};

PGNode * ClauseParser::buildMergedJoinVar(PGParseState * pstate,
        PGJoinType jointype, PGVar * l_colvar,
        PGVar * r_colvar)
{
    Oid outcoltype;
    int32 outcoltypmod;
    PGNode *l_node, *r_node, *res_node;

    /*
	 * Choose output type if input types are dissimilar.
	 */
    outcoltype = l_colvar->vartype;
    outcoltypmod = l_colvar->vartypmod;
    if (outcoltype != r_colvar->vartype)
    {
        outcoltype = coerce_parser_ptr->select_common_type(list_make2_oid(l_colvar->vartype, r_colvar->vartype), "JOIN/USING");
        outcoltypmod = -1; /* ie, unknown */
    }
    else if (outcoltypmod != r_colvar->vartypmod)
    {
        /* same type, but not same typmod */
        outcoltypmod = -1; /* ie, unknown */
    }

    /*
	 * Insert coercion functions if needed.  Note that a difference in typmod
	 * can only happen if input has typmod but outcoltypmod is -1. In that
	 * case we insert a RelabelType to clearly mark that result's typmod is
	 * not same as input.  We never need coerce_type_typmod.
	 */
    if (l_colvar->vartype != outcoltype)
        l_node = coerce_parser_ptr->coerce_type(
            pstate, (PGNode *)l_colvar, l_colvar->vartype, outcoltype, outcoltypmod, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
    else if (l_colvar->vartypmod != outcoltypmod)
        l_node = (PGNode *)makeRelabelType((PGExpr *)l_colvar, outcoltype, outcoltypmod, InvalidOid, PG_COERCE_IMPLICIT_CAST);
    else
        l_node = (PGNode *)l_colvar;

    if (r_colvar->vartype != outcoltype)
        r_node = coerce_parser_ptr->coerce_type(
            pstate, (PGNode *)r_colvar, r_colvar->vartype, outcoltype, outcoltypmod, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
    else if (r_colvar->vartypmod != outcoltypmod)
        r_node = (PGNode *)makeRelabelType((PGExpr *)r_colvar, outcoltype, outcoltypmod, InvalidOid, PG_COERCE_IMPLICIT_CAST);
    else
        r_node = (PGNode *)r_colvar;

    /*
	 * Choose what to emit
	 */
    switch (jointype)
    {
        case PG_JOIN_INNER:

            /*
			 * We can use either var; prefer non-coerced one if available.
			 */
            if (IsA(l_node, PGVar))
                res_node = l_node;
            else if (IsA(r_node, PGVar))
                res_node = r_node;
            else
                res_node = l_node;
            break;
        case PG_JOIN_LEFT:
            /* Always use left var */
            res_node = l_node;
            break;
        case PG_JOIN_RIGHT:
            /* Always use right var */
            res_node = r_node;
            break;
        case PG_JOIN_FULL: {
            /*
				 * Here we must build a COALESCE expression to ensure that the
				 * join output is non-null if either input is.
				 */
            PGCoalesceExpr * c = makeNode(PGCoalesceExpr);

            c->coalescetype = outcoltype;
            c->args = list_make2(l_node, r_node);
            res_node = (PGNode *)c;
            break;
        }
        default:
            elog(ERROR, "unrecognized join type: %d", (int)jointype);
            res_node = NULL; /* keep compiler quiet */
            break;
    }

    return res_node;
};

bool ClauseParser::interpretInhOption(InhOption inhOpt)
{
	switch (inhOpt)
	{
		case INH_NO:
			return false;
		case INH_YES:
			return true;
		case INH_DEFAULT:
			return SQL_inheritance;
	}
	elog(ERROR, "bogus InhOption value: %d", inhOpt);
	return false;				/* keep compiler quiet */
};

PGRangeTblEntry * ClauseParser::transformRangeSubselect(PGParseState * pstate,
        PGRangeSubselect * r)
{
    PGQuery * query;
    PGRangeTblEntry * rte;

    /*
	 * We require user to supply an alias for a subselect, per SQL92. To relax
	 * this, we'd have to be prepared to gin up a unique alias for an
	 * unlabeled subselect.  (This is just elog, not ereport, because the
	 * grammar should have enforced it already.  It'd probably be better to
	 * report the error here, but we don't have a good error location here.)
	 */
    if (r->alias == NULL)
        elog(ERROR, "subquery in FROM must have an alias");

    /*
	 * Set p_expr_kind to show this parse level is recursing to a subselect.
	 * We can't be nested within any expression, so don't need save-restore
	 * logic here.
	 */
    Assert(pstate->p_expr_kind == EXPR_KIND_NONE)
    pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

    /*
	 * If the subselect is LATERAL, make lateral_only names of this level
	 * visible to it.  (LATERAL can't nest within a single pstate level, so we
	 * don't need save/restore logic here.)
	 */
    Assert(!pstate->p_lateral_active);
    pstate->p_lateral_active = r->lateral;

    /*
	 * Analyze and transform the subquery.
	 */
    query = select_parser_ptr->parse_sub_analyze(r->subquery, pstate, NULL, getLockedRefname(pstate, r->alias->aliasname));

    /* Restore state */
    pstate->p_lateral_active = false;
    pstate->p_expr_kind = EXPR_KIND_NONE;

    /*
	 * Check that we got something reasonable.  Many of these conditions are
	 * impossible given restrictions of the grammar, but check 'em anyway.
	 */
    if (!IsA(query, PGQuery) || query->commandType != PG_CMD_SELECT || query->utilityStmt != NULL)
        elog(ERROR, "unexpected non-SELECT command in subquery in FROM");

    /*
	 * OK, build an RTE for the subquery.
	 */
    rte = relation_parser_ptr->addRangeTableEntryForSubquery(pstate, query, r->alias, r->lateral, true);
    return rte;
};

PGRangeTblEntry * ClauseParser::transformTableEntry(PGParseState * pstate,
        PGRangeVar * r)
{
    PGRangeTblEntry * rte;

    /* We need only build a range table entry */
    rte = relation_parser_ptr->addRangeTableEntry(pstate, r, r->alias, interpretInhOption(r->inhOpt), true);

    return rte;
};

PGNode *
ClauseParser::transformWhereClause(PGParseState * pstate, PGNode * clause, PGParseExprKind exprKind, const char * constructName)
{
    PGNode * qual;

    if (clause == NULL)
        return NULL;

    qual = expr_parser_ptr->transformExpr(pstate, clause, exprKind);

    qual = coerce_parser_ptr->coerce_to_boolean(pstate, qual, constructName);

    return qual;
};

// PGRangeTblEntry * ClauseParser::transformRangeFunction(PGParseState * pstate, PGRangeFunction * r)
// {
//     PGNode * funcexpr;
//     char * funcname;
//     PGRangeTblEntry * rte;

//     /*
// 	 * Get function name for possible use as alias.  We use the same
// 	 * transformation rules as for a SELECT output expression.	For a FuncCall
// 	 * node, the result will be the function name, but it is possible for the
// 	 * grammar to hand back other node types.
// 	 */
//     funcname = target_parser_ptr->FigureColname(r->funccallnode);

//     if (funcname)
//     {
//         if (pg_strncasecmp(funcname, GP_DIST_RANDOM_NAME, sizeof(GP_DIST_RANDOM_NAME)) == 0)
//         {
//             /* OK, now we need to check the arguments and generate a RTE */
//             PGFuncCall * fc;
//             PGRangeVar * rel;

//             fc = (PGFuncCall *)r->funccallnode;

//             if (list_length(fc->args) != 1)
//                 elog(ERROR, "Invalid %s syntax.", GP_DIST_RANDOM_NAME);

//             if (IsA(linitial(fc->args), PGAConst))
//             {
//                 PGAConst * arg_val;
//                 char * schemaname;
//                 char * tablename;

//                 arg_val = linitial(fc->args);
//                 if (!IsA(&arg_val->val, String))
//                 {
//                     elog(ERROR, "%s: invalid argument type, non-string in value", GP_DIST_RANDOM_NAME);
//                 }

//                 schemaname = strVal(&arg_val->val);
//                 tablename = strchr(schemaname, '.');
//                 if (tablename)
//                 {
//                     *tablename = 0;
//                     tablename++;
//                 }
//                 else
//                 {
//                     /* no schema */
//                     tablename = schemaname;
//                     schemaname = NULL;
//                 }

//                 /* Got the name of the table, now we need to build the RTE for the table. */
//                 rel = makeRangeVar(NULL /*catalogname*/, schemaname, tablename, arg_val->location);
//                 rel->location = arg_val->location;

//                 rte = relation_parser_ptr->addRangeTableEntry(pstate, rel, r->alias, false, true);

//                 /* Now we set our special attribute in the rte. */
//                 rte->forceDistRandom = true;

//                 return rte;
//             }
//             else
//             {
//                 elog(ERROR, "%s: invalid argument type", GP_DIST_RANDOM_NAME);
//             }
//         }
//     }

//     /*
// 	 * Transform the raw expression.
// 	 */
//     funcexpr = expr_parser_ptr->transformExpr(pstate, r->funccallnode);

//     /*
// 	 * The function parameters cannot make use of any variables from other
// 	 * FROM items.	(Compare to transformRangeSubselect(); the coding is
// 	 * different though because we didn't parse as a sub-select with its own
// 	 * level of namespace.)
// 	 *
// 	 * XXX this will need further work to support SQL99's LATERAL() feature,
// 	 * wherein such references would indeed be legal.
// 	 */
//     if (pstate->p_relnamespace || pstate->p_varnamespace)
//     {
//         if (contain_vars_of_level(funcexpr, 0))
//             ereport(
//                 ERROR,
//                 (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
//                  errmsg("function expression in FROM may not refer to other relations of same query level"),
//                  errOmitLocation(true)));
//     }

//     /*
// 	 * Disallow aggregate functions in the expression.	(No reason to postpone
// 	 * this check until parseCheckAggregates.)
// 	 */
//     if (pstate->p_hasAggs)
//     {
//         if (checkExprHasAggs(funcexpr))
//             ereport(
//                 ERROR,
//                 (errcode(ERRCODE_GROUPING_ERROR),
//                  errmsg("cannot use aggregate function in function expression in FROM"),
//                  errOmitLocation(true)));
//     }

//     /*
// 	 * OK, build an RTE for the function.
// 	 */
//     rte = relation_parser_ptr->addRangeTableEntryForFunction(pstate, funcname, funcexpr, r, true);

//     /*
// 	 * If a coldeflist was supplied, ensure it defines a legal set of names
// 	 * (no duplicates) and datatypes (no pseudo-types, for instance).
// 	 * addRangeTableEntryForFunction looked up the type names but didn't check
// 	 * them further than that.
// 	 */
//     if (r->coldeflist)
//     {
//         TupleDesc tupdesc;

//         tupdesc = BuildDescFromLists(rte->eref->colnames, rte->funccoltypes, rte->funccoltypmods);
//         CheckAttributeNamesTypes(tupdesc, RELKIND_COMPOSITE_TYPE);
//     }

//     return rte;
// };

PGNode *
ClauseParser::transformJoinUsingClause(PGParseState * pstate, PGRangeTblEntry * leftRTE,
    PGRangeTblEntry * rightRTE, PGList * leftVars,
    PGList * rightVars)
{
    PGNode * result = NULL;
    PGListCell *lvars, *rvars;

    /*
	 * We cheat a little bit here by building an untransformed operator tree
	 * whose leaves are the already-transformed Vars.  This is OK because
	 * transformExpr() won't complain about already-transformed subnodes.
	 */
    forboth(lvars, leftVars, rvars, rightVars)
    {
        PGNode * lvar = (PGNode *)lfirst(lvars);
        PGNode * rvar = (PGNode *)lfirst(rvars);
        PGAExpr * e;

        e = makeSimpleAExpr(PG_AEXPR_OP, "=", (PGNode *)copyObject(lvar), (PGNode *)copyObject(rvar), -1);

        if (result == NULL)
            result = (PGNode *)e;
        else
        {
            PGExpr * a;

            //a = makeAExpr(AEXPR_AND, NIL, result, (PGNode *)e, -1);
            a = makeBoolExpr(PG_AND_EXPR, list_make2(result, (PGNode *)e), -1);
            result = (PGNode *)a;
        }
    }

    /*
	 * Since the references are already Vars, and are certainly from the input
	 * relations, we don't have to go through the same pushups that
	 * transformJoinOnClause() does.  Just invoke transformExpr() to fix up
	 * the operators, and we're done.
	 */
    result = expr_parser_ptr->transformExpr(pstate, result);

    result = coerce_parser_ptr->coerce_to_boolean(pstate, result, "JOIN/USING");

    return result;
};

PGNode * transformJoinOnClause(PGParseState * pstate,
    PGJoinExpr * j, PGList * namespace)
{
    PGNode * result;
    PGList * save_namespace;

    /*
	 * The namespace that the join expression should see is just the two
	 * subtrees of the JOIN plus any outer references from upper pstate
	 * levels.  Temporarily set this pstate's namespace accordingly.  (We need
	 * not check for refname conflicts, because transformFromClauseItem()
	 * already did.)  All namespace items are marked visible regardless of
	 * LATERAL state.
	 */
    setNamespaceLateralState(namespace, false, true);

    save_namespace = pstate->p_namespace;
    pstate->p_namespace = namespace;

    result = transformWhereClause(pstate, j->quals, EXPR_KIND_JOIN_ON, "JOIN/ON");

    pstate->p_namespace = save_namespace;

    return result;
};

void ClauseParser::extractRemainingColumns(PGList * common_colnames, PGList * src_colnames,
        PGList * src_colvars, PGList ** res_colnames,
        PGList ** res_colvars)
{
    PGList * new_colnames = NIL;
    PGList * new_colvars = NIL;
    PGListCell *lnames, *lvars;

    Assert(list_length(src_colnames) == list_length(src_colvars))

    forboth(lnames, src_colnames, lvars, src_colvars)
    {
        char * colname = strVal(lfirst(lnames));
        bool match = false;
        PGListCell * cnames;

        foreach (cnames, common_colnames)
        {
            char * ccolname = strVal(lfirst(cnames));

            if (strcmp(colname, ccolname) == 0)
            {
                match = true;
                break;
            }
        }

        if (!match)
        {
            new_colnames = lappend(new_colnames, lfirst(lnames));
            new_colvars = lappend(new_colvars, lfirst(lvars));
        }
    }

    *res_colnames = new_colnames;
    *res_colvars = new_colvars;
};

PGRangeTblEntry * ClauseParser::transformCTEReference(PGParseState * pstate,
    PGRangeVar * r, PGCommonTableExpr * cte, Index levelsup)
{
    PGRangeTblEntry * rte;

    rte = cte_parser_ptr->addRangeTableEntryForCTE(pstate, cte, levelsup, r, true);

    return rte;
};

void ClauseParser::setNamespaceLateralState(PGList * namespace, bool lateral_only, bool lateral_ok)
{
    PGListCell * lc;

    foreach (lc, namespace)
    {
        PGParseNamespaceItem * nsitem = (PGParseNamespaceItem *)lfirst(lc);

        nsitem->p_lateral_only = lateral_only;
        nsitem->p_lateral_ok = lateral_ok;
    }
};

PGNode * ClauseParser::transformFromClauseItem(PGParseState * pstate, PGNode * n,
    PGRangeTblEntry ** top_rte, int * top_rti, PGList ** namespace)
{
    PGNode * result;

    if (IsA(n, PGRangeVar))
    {
        /* Plain relation reference, or perhaps a CTE reference */
        PGRangeVar * rv = (PGRangeVar *)n;
        PGRangeTblRef * rtr;
        PGRangeTblEntry * rte = NULL;
        int rtindex;

        /* if it is an unqualified name, it might be a CTE reference */
        if (!rv->schemaname)
        {
            PGCommonTableExpr * cte;
            Index levelsup;

            cte = relation_parser_ptr->scanNameSpaceForCTE(pstate, rv->relname, &levelsup);
            if (cte)
                rte = transformCTEReference(pstate, rv, cte, levelsup);
        }

        /* if not found as a CTE, must be a table reference */
        if (!rte)
            rte = transformTableEntry(pstate, rv);

        /* assume new rte is at end */
        rtindex = list_length(pstate->p_rtable);
        Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
        *top_rte = rte;
        *top_rti = rtindex;
        *namespace = list_make1(makeDefaultNSItem(rte));
        rtr = makeNode(PGRangeTblRef);
        rtr->rtindex = rtindex;
        result = (PGNode *)rtr;
    }
    else if (IsA(n, PGRangeSubselect))
    {
        /* sub-SELECT is like a plain relation */
        RangeTblRef * rtr;
        RangeTblEntry * rte;
        int rtindex;

        rte = transformRangeSubselect(pstate, (RangeSubselect *)n);
        /* assume new rte is at end */
        rtindex = list_length(pstate->p_rtable);
        Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
        *top_rte = rte;
        *top_rti = rtindex;
        *namespace = list_make1(makeDefaultNSItem(rte));
        rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;
        result = (Node *)rtr;
    }
    // else if (IsA(n, RangeFunction))
    // {
    //     /* function is like a plain relation */
    //     RangeTblRef * rtr;
    //     RangeTblEntry * rte;
    //     int rtindex;

    //     rte = transformRangeFunction(pstate, (RangeFunction *)n);
    //     /* assume new rte is at end */
    //     rtindex = list_length(pstate->p_rtable);
    //     Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
    //     *top_rte = rte;
    //     *top_rti = rtindex;
    //     *namespace = list_make1(makeDefaultNSItem(rte));
    //     rtr = makeNode(RangeTblRef);
    //     rtr->rtindex = rtindex;
    //     result = (Node *)rtr;
    // }
    else if (IsA(n, PGJoinExpr))
    {
        /* A newfangled join expression */
        PGJoinExpr * j = (PGJoinExpr *)n;
        PGRangeTblEntry * l_rte;
        PGRangeTblEntry * r_rte;
        int l_rtindex;
        int r_rtindex;
        PGList *l_namespace, *r_namespace, *my_namespace, *l_colnames, *r_colnames, *res_colnames, *l_colvars, *r_colvars, *res_colvars;
        bool lateral_ok;
        int sv_namespace_length;
        PGRangeTblEntry * rte;
        int k;

        /*
		 * Recursively process the left subtree, then the right.  We must do
		 * it in this order for correct visibility of LATERAL references.
		 */
        j->larg = transformFromClauseItem(pstate, j->larg, &l_rte, &l_rtindex, &l_namespace);

        /*
		 * Make the left-side RTEs available for LATERAL access within the
		 * right side, by temporarily adding them to the pstate's namespace
		 * list.  Per SQL:2008, if the join type is not INNER or LEFT then the
		 * left-side names must still be exposed, but it's an error to
		 * reference them.  (Stupid design, but that's what it says.)  Hence,
		 * we always push them into the namespace, but mark them as not
		 * lateral_ok if the jointype is wrong.
		 *
		 * Notice that we don't require the merged namespace list to be
		 * conflict-free.  See the comments for scanNameSpaceForRefname().
		 *
		 * NB: this coding relies on the fact that list_concat is not
		 * destructive to its second argument.
		 */
        lateral_ok = (j->jointype == PG_JOIN_INNER || j->jointype == PG_JOIN_LEFT);
        setNamespaceLateralState(l_namespace, true, lateral_ok);

        sv_namespace_length = list_length(pstate->p_namespace);
        pstate->p_namespace = list_concat(pstate->p_namespace, l_namespace);

        /* And now we can process the RHS */
        j->rarg = transformFromClauseItem(pstate, j->rarg, &r_rte, &r_rtindex, &r_namespace);

        /* Remove the left-side RTEs from the namespace list again */
        pstate->p_namespace = list_truncate(pstate->p_namespace, sv_namespace_length);

        /*
		 * Check for conflicting refnames in left and right subtrees. Must do
		 * this because higher levels will assume I hand back a self-
		 * consistent namespace list.
		 */
        relation_parser_ptr->checkNameSpaceConflicts(pstate, l_namespace, r_namespace);

        /*
		 * Generate combined namespace info for possible use below.
		 */
        my_namespace = list_concat(l_namespace, r_namespace);

        /*
		 * Extract column name and var lists from both subtrees
		 *
		 * Note: expandRTE returns new lists, safe for me to modify
		 */
        relation_parser_ptr->expandRTE(l_rte, l_rtindex, 0, -1, false, &l_colnames, &l_colvars);
        relation_parser_ptr->expandRTE(r_rte, r_rtindex, 0, -1, false, &r_colnames, &r_colvars);

        /*
		 * Natural join does not explicitly specify columns; must generate
		 * columns to join. Need to run through the list of columns from each
		 * table or join result and match up the column names. Use the first
		 * table, and check every column in the second table for a match.
		 * (We'll check that the matches were unique later on.) The result of
		 * this step is a list of column names just like an explicitly-written
		 * USING list.
		 */
        if (j->isNatural)
        {
            PGList * rlist = NIL;
            PGListCell *lx, *rx;

            Assert(j->usingClause == NIL) /* shouldn't have USING() too */

            foreach (lx, l_colnames)
            {
                char * l_colname = strVal(lfirst(lx));
                PGValue * m_name = NULL;

                foreach (rx, r_colnames)
                {
                    char * r_colname = strVal(lfirst(rx));

                    if (strcmp(l_colname, r_colname) == 0)
                    {
                        m_name = makeString(l_colname);
                        break;
                    }
                }

                /* matched a right column? then keep as join column... */
                if (m_name != NULL)
                    rlist = lappend(rlist, m_name);
            }

            j->usingClause = rlist;
        }

        /*
		 * Now transform the join qualifications, if any.
		 */
        res_colnames = NIL;
        res_colvars = NIL;

        if (j->usingClause)
        {
            /*
			 * JOIN/USING (or NATURAL JOIN, as transformed above). Transform
			 * the list into an explicit ON-condition, and generate a list of
			 * merged result columns.
			 */
            PGList * ucols = j->usingClause;
            PGList * l_usingvars = NIL;
            PGList * r_usingvars = NIL;
            PGListCell * ucol;

            Assert(j->quals == NULL); /* shouldn't have ON() too */

            foreach (ucol, ucols)
            {
                char * u_colname = strVal(lfirst(ucol));
                PGListCell * col;
                int ndx;
                int l_index = -1;
                int r_index = -1;
                PGVar *l_colvar, *r_colvar;

                /* Check for USING(foo,foo) */
                foreach (col, res_colnames)
                {
                    char * res_colname = strVal(lfirst(col));

                    if (strcmp(res_colname, u_colname) == 0)
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_DUPLICATE_COLUMN),
                             errmsg("column name \"%s\" appears more than once in USING clause", u_colname)));
                }

                /* Find it in left input */
                ndx = 0;
                foreach (col, l_colnames)
                {
                    char * l_colname = strVal(lfirst(col));

                    if (strcmp(l_colname, u_colname) == 0)
                    {
                        if (l_index >= 0)
                            ereport(
                                ERROR,
                                (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                                 errmsg("common column name \"%s\" appears more than once in left table", u_colname)));
                        l_index = ndx;
                    }
                    ndx++;
                }
                if (l_index < 0)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                         errmsg("column \"%s\" specified in USING clause does not exist in left table", u_colname)));

                /* Find it in right input */
                ndx = 0;
                foreach (col, r_colnames)
                {
                    char * r_colname = strVal(lfirst(col));

                    if (strcmp(r_colname, u_colname) == 0)
                    {
                        if (r_index >= 0)
                            ereport(
                                ERROR,
                                (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                                 errmsg("common column name \"%s\" appears more than once in right table", u_colname)));
                        r_index = ndx;
                    }
                    ndx++;
                }
                if (r_index < 0)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                         errmsg("column \"%s\" specified in USING clause does not exist in right table", u_colname)));

                l_colvar = list_nth(l_colvars, l_index);
                l_usingvars = lappend(l_usingvars, l_colvar);
                r_colvar = list_nth(r_colvars, r_index);
                r_usingvars = lappend(r_usingvars, r_colvar);

                res_colnames = lappend(res_colnames, lfirst(ucol));
                res_colvars = lappend(res_colvars, buildMergedJoinVar(pstate, j->jointype, l_colvar, r_colvar));
            }

            j->quals = transformJoinUsingClause(pstate, l_rte, r_rte, l_usingvars, r_usingvars);
        }
        else if (j->quals)
        {
            /* User-written ON-condition; transform it */
            j->quals = transformJoinOnClause(pstate, j, my_namespace);
        }
        else
        {
            /* CROSS JOIN: no quals */
        }

        /* Add remaining columns from each side to the output columns */
        extractRemainingColumns(res_colnames, l_colnames, l_colvars, &l_colnames, &l_colvars);
        extractRemainingColumns(res_colnames, r_colnames, r_colvars, &r_colnames, &r_colvars);
        res_colnames = list_concat(res_colnames, l_colnames);
        res_colvars = list_concat(res_colvars, l_colvars);
        res_colnames = list_concat(res_colnames, r_colnames);
        res_colvars = list_concat(res_colvars, r_colvars);

        /*
		 * Check alias (AS clause), if any.
		 */
        if (j->alias)
        {
            if (j->alias->colnames != NIL)
            {
                if (list_length(j->alias->colnames) > list_length(res_colnames))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("column alias list for \"%s\" has too many entries", j->alias->aliasname)));
            }
        }

        /*
		 * Now build an RTE for the result of the join
		 */
        rte = addRangeTableEntryForJoin(pstate, res_colnames, j->jointype, res_colvars, j->alias, true);

        /* assume new rte is at end */
        j->rtindex = list_length(pstate->p_rtable);
        Assert(rte == rt_fetch(j->rtindex, pstate->p_rtable));

        *top_rte = rte;
        *top_rti = j->rtindex;

        /* make a matching link to the JoinExpr for later use */
        for (k = list_length(pstate->p_joinexprs) + 1; k < j->rtindex; k++)
            pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
        pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
        Assert(list_length(pstate->p_joinexprs) == j->rtindex);

        /*
		 * Prepare returned namespace list.  If the JOIN has an alias then it
		 * hides the contained RTEs completely; otherwise, the contained RTEs
		 * are still visible as table names, but are not visible for
		 * unqualified column-name access.
		 *
		 * Note: if there are nested alias-less JOINs, the lower-level ones
		 * will remain in the list although they have neither p_rel_visible
		 * nor p_cols_visible set.  We could delete such list items, but it's
		 * unclear that it's worth expending cycles to do so.
		 */
        if (j->alias != NULL)
            my_namespace = NIL;
        else
            setNamespaceColumnVisibility(my_namespace, false);

        /*
		 * The join RTE itself is always made visible for unqualified column
		 * names.  It's visible as a relation name only if it has an alias.
		 */
        *namespace = lappend(my_namespace, makeNamespaceItem(rte, (j->alias != NULL), true, false, true));

        result = (Node *)j;
    }
    else
    {
        result = NULL;
        elog(ERROR, "unrecognized node type: %d", (int)nodeTag(n));
    }

    return result;
};

void ClauseParser::transformFromClause(PGParseState * pstate, PGList * frmList)
{
    PGListCell * fl;

    /*
	 * The grammar will have produced a list of RangeVars, RangeSubselects,
	 * RangeFunctions, and/or JoinExprs. Transform each one (possibly adding
	 * entries to the rtable), check for duplicate refnames, and then add it
	 * to the joinlist and namespace.
	 *
	 * Note we must process the items left-to-right for proper handling of
	 * LATERAL references.
	 */
    foreach (fl, frmList)
    {
        PGNode * n = lfirst(fl);
        PGRangeTblEntry * rte = NULL;
        int rtindex = 0;
        PGList * namespace = NULL;

        n = transformFromClauseItem(pstate, n, &rte, &rtindex, &namespace);

        relation_parser_ptr->checkNameSpaceConflicts(pstate, pstate->p_namespace, namespace);

        /* Mark the new namespace items as visible only to LATERAL */
        setNamespaceLateralState(namespace, true, true);

        pstate->p_joinlist = lappend(pstate->p_joinlist, n);
        pstate->p_namespace = list_concat(pstate->p_namespace, namespace);
    }

    /*
	 * We're done parsing the FROM list, so make all namespace items
	 * unconditionally visible.  Note that this will also reset lateral_only
	 * for any namespace items that were already present when we were called;
	 * but those should have been that way already.
	 */
    setNamespaceLateralState(pstate->p_namespace, false, true);
};

PGTargetEntry * findTargetlistEntrySQL99(PGParseState * pstate,
    PGNode * node, PGList ** tlist, PGParseExprKind exprKind)
{
    PGTargetEntry * target_result;
    PGListCell * tl;
    PGNode * expr;

    /*
	 * Convert the untransformed node to a transformed expression, and search
	 * for a match in the tlist.  NOTE: it doesn't really matter whether there
	 * is more than one match.  Also, we are willing to match an existing
	 * resjunk target here, though the SQL92 cases above must ignore resjunk
	 * targets.
	 */
    expr = expr_parser_ptr->transformExpr(pstate, node, exprKind);

    foreach (tl, *tlist)
    {
        PGTargetEntry * tle = (PGTargetEntry *)lfirst(tl);
        PGNode * texpr;

        /*
		 * Ignore any implicit cast on the existing tlist expression.
		 *
		 * This essentially allows the ORDER/GROUP/etc item to adopt the same
		 * datatype previously selected for a textually-equivalent tlist item.
		 * There can't be any implicit cast at top level in an ordinary SELECT
		 * tlist at this stage, but the case does arise with ORDER BY in an
		 * aggregate function.
		 */
        texpr = strip_implicit_coercions((PGNode *)tle->expr);

        if (equal(expr, texpr))
            return tle;
    }

    /*
	 * If no matches, construct a new target entry which is appended to the
	 * end of the target list.  This target is given resjunk = TRUE so that it
	 * will not be projected into the final tuple.
	 */
    target_result = target_parser_ptr->transformTargetEntry(pstate, node, expr, exprKind, NULL, true);

    *tlist = lappend(*tlist, target_result);

    return target_result;
};

PGTargetEntry * ClauseParser::findTargetlistEntrySQL92(
      PGParseState * pstate, PGNode * node, PGList ** tlist, PGParseExprKind exprKind)
{
    PGListCell * tl;

    /*----------
	 * Handle two special cases as mandated by the SQL92 spec:
	 *
	 * 1. Bare ColumnName (no qualifier or subscripts)
	 *	  For a bare identifier, we search for a matching column name
	 *	  in the existing target list.  Multiple matches are an error
	 *	  unless they refer to identical values; for example,
	 *	  we allow	SELECT a, a FROM table ORDER BY a
	 *	  but not	SELECT a AS b, b FROM table ORDER BY b
	 *	  If no match is found, we fall through and treat the identifier
	 *	  as an expression.
	 *	  For GROUP BY, it is incorrect to match the grouping item against
	 *	  targetlist entries: according to SQL92, an identifier in GROUP BY
	 *	  is a reference to a column name exposed by FROM, not to a target
	 *	  list column.  However, many implementations (including pre-7.0
	 *	  PostgreSQL) accept this anyway.  So for GROUP BY, we look first
	 *	  to see if the identifier matches any FROM column name, and only
	 *	  try for a targetlist name if it doesn't.  This ensures that we
	 *	  adhere to the spec in the case where the name could be both.
	 *	  DISTINCT ON isn't in the standard, so we can do what we like there;
	 *	  we choose to make it work like ORDER BY, on the rather flimsy
	 *	  grounds that ordinary DISTINCT works on targetlist entries.
	 *
	 * 2. IntegerConstant
	 *	  This means to use the n'th item in the existing target list.
	 *	  Note that it would make no sense to order/group/distinct by an
	 *	  actual constant, so this does not create a conflict with SQL99.
	 *	  GROUP BY column-number is not allowed by SQL92, but since
	 *	  the standard has no other behavior defined for this syntax,
	 *	  we may as well accept this common extension.
	 *
	 * Note that pre-existing resjunk targets must not be used in either case,
	 * since the user didn't write them in his SELECT list.
	 *
	 * If neither special case applies, fall through to treat the item as
	 * an expression per SQL99.
	 *----------
	 */
    if (IsA(node, PGColumnRef) && list_length(((PGColumnRef *)node)->fields) == 1 && IsA(linitial(((PGColumnRef *)node)->fields), String))
    {
        char * name = strVal(linitial(((PGColumnRef *)node)->fields));
        int location = ((PGColumnRef *)node)->location;

        if (exprKind == EXPR_KIND_GROUP_BY)
        {
            /*
			 * In GROUP BY, we must prefer a match against a FROM-clause
			 * column to one against the targetlist.  Look to see if there is
			 * a matching column.  If so, fall through to use SQL99 rules.
			 * NOTE: if name could refer ambiguously to more than one column
			 * name exposed by FROM, colNameToVar will ereport(ERROR). That's
			 * just what we want here.
			 *
			 * Small tweak for 7.4.3: ignore matches in upper query levels.
			 * This effectively changes the search order for bare names to (1)
			 * local FROM variables, (2) local targetlist aliases, (3) outer
			 * FROM variables, whereas before it was (1) (3) (2). SQL92 and
			 * SQL99 do not allow GROUPing BY an outer reference, so this
			 * breaks no cases that are legal per spec, and it seems a more
			 * self-consistent behavior.
			 */
            if (colNameToVar(pstate, name, true, location) != NULL)
                name = NULL;
        }

        if (name != NULL)
        {
            PGTargetEntry * target_result = NULL;

            foreach (tl, *tlist)
            {
                PGTargetEntry * tle = (PGTargetEntry *)lfirst(tl);

                if (!tle->resjunk && strcmp(tle->resname, name) == 0)
                {
                    if (target_result != NULL)
                    {
                        if (!equal(target_result->expr, tle->expr))
                            ereport(
                                ERROR,
                                (errcode(ERRCODE_AMBIGUOUS_COLUMN),

                                 /*------
							  translator: first %s is name of a SQL construct, eg ORDER BY */
                                 errmsg("%s \"%s\" is ambiguous", ParseExprKindName(exprKind), name),
                                 parser_errposition(pstate, location)));
                    }
                    else
                        target_result = tle;
                    /* Stay in loop to check for ambiguity */
                }
            }
            if (target_result != NULL)
            {
                /* return the first match, after suitable validation */
                checkTargetlistEntrySQL92(pstate, target_result, exprKind);
                return target_result;
            }
        }
    }
    if (IsA(node, PGAConst))
    {
        PGValue * val = &((PGAConst *)node)->val;
        int location = ((PGAConst *)node)->location;
        int targetlist_pos = 0;
        int target_pos;

        if (!IsA(val, PGInteger))
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 /* translator: %s is name of a SQL construct, eg ORDER BY */
                 errmsg("non-integer constant in %s", ParseExprKindName(exprKind)),
                 parser_errposition(pstate, location)));

        target_pos = intVal(val);
        foreach (tl, *tlist)
        {
            PGTargetEntry * tle = (PGTargetEntry *)lfirst(tl);

            if (!tle->resjunk)
            {
                if (++targetlist_pos == target_pos)
                {
                    /* return the unique match, after suitable validation */
                    checkTargetlistEntrySQL92(pstate, tle, exprKind);
                    return tle;
                }
            }
        }
        ereport(
            ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
             /* translator: %s is name of a SQL construct, eg ORDER BY */
             errmsg("%s position %d is not in select list", ParseExprKindName(exprKind), target_pos),
             parser_errposition(pstate, location)));
    }

    /*
	 * Otherwise, we have an expression, so process it per SQL99 rules.
	 */
    return findTargetlistEntrySQL99(pstate, node, tlist, exprKind);
};

bool ClauseParser::targetIsInSortGroupList(PGTargetEntry * tle, Oid sortop,
        PGList * sortgroupList)
{
    Index ref = tle->ressortgroupref;
    PGListCell * l;

    /* no need to scan list if tle has no marker */
    if (ref == 0)
        return false;

    foreach (l, sortgroupList)
    {
        PGNode * node = (PGNode *)lfirst(l);

        /* Skip the empty grouping set */
        if (node == NULL)
            continue;

        //TODO
        // if (IsA(node, GroupClause) || IsA(node, SortClause))
        // {
        //     GroupClause * gc = (GroupClause *)node;
        //     if (gc->tleSortGroupRef == ref)
        //         return true;
        // }

        if (IsA(node, PGSortGroupClause))
        {
            PGSortGroupClause * scl = (PGSortGroupClause *)node;

            if (scl->tleSortGroupRef == ref && (sortop == InvalidOid || sortop == scl->sortop || sortop == get_commutator(scl->sortop)))
                return true;
        }
    }
    return false;
};

Index ClauseParser::assignSortGroupRef(PGTargetEntry * tle, PGList * tlist)
{
    Index maxRef;
    PGListCell * l;

    if (tle->ressortgroupref) /* already has one? */
        return tle->ressortgroupref;

    /* easiest way to pick an unused refnumber: max used + 1 */
    maxRef = 0;
    foreach (l, tlist)
    {
        Index ref = ((PGTargetEntry *)lfirst(l))->ressortgroupref;

        if (ref > maxRef)
            maxRef = ref;
    }
    tle->ressortgroupref = maxRef + 1;
    return tle->ressortgroupref;
};

PGList * ClauseParser::addTargetToSortList(
      PGParseState * pstate,
      PGTargetEntry * tle,
      PGList * sortlist,
      PGList * targetlist,
      PGSortBy * sortby,
      bool resolveUnknown)
{
    Oid restype = exprType((PGNode *)tle->expr);
    Oid sortop;
    Oid eqop;
    bool hashable;
    bool reverse;
    int location;
    PGParseCallbackState pcbstate;

    /* if tlist item is an UNKNOWN literal, change it to TEXT */
    if (restype == UNKNOWNOID && resolveUnknown)
    {
        tle->expr = (PGExpr *)coerce_parser_ptr->coerce_type(
            pstate, (PGNode *)tle->expr, restype, TEXTOID, -1, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, exprLocation((PGNode *)tle->expr));
        restype = TEXTOID;
    }

    /*
	 * Rather than clutter the API of get_sort_group_operators and the other
	 * functions we're about to use, make use of error context callback to
	 * mark any error reports with a parse position.  We point to the operator
	 * location if present, else to the expression being sorted.  (NB: use the
	 * original untransformed expression here; the TLE entry might well point
	 * at a duplicate expression in the regular SELECT list.)
	 */
    location = sortby->location;
    if (location < 0)
        location = exprLocation(sortby->node);
    setup_parser_errposition_callback(&pcbstate, pstate, location);

    /* determine the sortop, eqop, and directionality */
    switch (sortby->sortby_dir)
    {
        case PG_SORTBY_DEFAULT:
        case PG_SORTBY_ASC:
            get_sort_group_operators(restype, true, true, false, &sortop, &eqop, NULL, &hashable);
            reverse = false;
            break;
        case PG_SORTBY_DESC:
            get_sort_group_operators(restype, false, true, true, NULL, &eqop, &sortop, &hashable);
            reverse = true;
            break;
        case SORTBY_USING:
            Assert(sortby->useOp != NIL);
            sortop = compatible_oper_opid(sortby->useOp, restype, restype, false);

            /*
			 * Verify it's a valid ordering operator, fetch the corresponding
			 * equality operator, and determine whether to consider it like
			 * ASC or DESC.
			 */
            eqop = get_equality_op_for_ordering_op(sortop, &reverse);
            if (!OidIsValid(eqop))
                ereport(
                    ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("operator %s is not a valid ordering operator", strVal(llast(sortby->useOp))),
                     errhint("Ordering operators must be \"<\" or \">\" members of btree operator families.")));

            /*
			 * Also see if the equality operator is hashable.
			 */
            hashable = op_hashjoinable(eqop, restype);
            break;
        default:
            elog(ERROR, "unrecognized sortby_dir: %d", sortby->sortby_dir);
            sortop = InvalidOid; /* keep compiler quiet */
            eqop = InvalidOid;
            hashable = false;
            reverse = false;
            break;
    }

    cancel_parser_errposition_callback(&pcbstate);

    /* avoid making duplicate sortlist entries */
    if (!targetIsInSortList(tle, sortop, sortlist))
    {
        PGSortGroupClause * sortcl = makeNode(PGSortGroupClause);

        sortcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);

        sortcl->eqop = eqop;
        sortcl->sortop = sortop;
        sortcl->hashable = hashable;

        switch (sortby->sortby_nulls)
        {
            case PG_SORTBY_NULLS_DEFAULT:
                /* NULLS FIRST is default for DESC; other way for ASC */
                sortcl->nulls_first = reverse;
                break;
            case PG_SORTBY_NULLS_FIRST:
                sortcl->nulls_first = true;
                break;
            case PG_SORTBY_NULLS_LAST:
                sortcl->nulls_first = false;
                break;
            default:
                elog(ERROR, "unrecognized sortby_nulls: %d", sortby->sortby_nulls);
                break;
        }

        sortlist = lappend(sortlist, sortcl);
    }

    return sortlist;
};

PGList * ClauseParser::transformSortClause(
      PGParseState * pstate, PGList * orderlist,
      PGList ** targetlist,
      PGParseExprKind exprKind, bool resolveUnknown, bool useSQL99)
{
    PGList * sortlist = NIL;
    PGListCell * olitem;

    foreach (olitem, orderlist)
    {
        PGSortBy * sortby = (PGSortBy *)lfirst(olitem);
        PGTargetEntry * tle;

        if (useSQL99)
            tle = findTargetlistEntrySQL99(pstate, sortby->node, targetlist, exprKind);
        else
            tle = findTargetlistEntrySQL92(pstate, sortby->node, targetlist, exprKind);

        sortlist = addTargetToSortList(pstate, tle, sortlist, *targetlist, sortby, resolveUnknown);
    }

    return sortlist;
};

PGList * ClauseParser::findListTargetlistEntries(PGParseState * pstate, PGNode * node,
        PGList ** tlist, bool in_grpext, bool ignore_in_grpext, bool useSQL99)
{
    PGList * result_list = NIL;

    /*
	 * In GROUP BY clauses, empty grouping set () is supported as 'NIL'
	 * in the list. If this is the case, we simply skip it.
	 */
    if (node == NULL)
        return result_list;

    if (IsA(node, GroupingClause))
    {
        PGListCell * gl;
        GroupingClause * gc = (GroupingClause *)node;

        foreach (gl, gc->groupsets)
        {
            PGList * subresult_list;

            subresult_list = findListTargetlistEntries(pstate, lfirst(gl), tlist, true, ignore_in_grpext, useSQL99);

            result_list = list_concat(result_list, subresult_list);
        }
    }

    /*
	 * In GROUP BY clause, we handle RowExpr specially here. When
	 * RowExprs appears immediately inside a grouping extension, we do
	 * not want to append the target entries for their arguments into
	 * result_list. This is because we do not want the order of
	 * these target entries in the result list from transformGroupClause()
	 * to be affected by ORDER BY.
	 *
	 * However, if ignore_in_grpext is set, we will always append
	 * these target enties.
	 */
    else if (IsA(node, PGRowExpr))
    {
        PGList * args = ((PGRowExpr *)node)->args;
        PGListCell * lc;

        foreach (lc, args)
        {
            PGNode * rowexpr_arg = lfirst(lc);
            PGTargetEntry * tle;

            if (useSQL99)
                tle = findTargetlistEntrySQL99(pstate, rowexpr_arg, tlist);
            else
                tle = findTargetlistEntrySQL92(pstate, rowexpr_arg, tlist, GROUP_CLAUSE);

            /* If RowExpr does not appear immediately inside a GROUPING SETS,
			 * we append its targetlit to the given targetlist.
			 */
            if (ignore_in_grpext || !in_grpext)
                result_list = lappend(result_list, tle);
        }
    }

    else
    {
        PGTargetEntry * tle;

        if (useSQL99)
            tle = findTargetlistEntrySQL99(pstate, node, tlist);
        else
            tle = findTargetlistEntrySQL92(pstate, node, tlist, GROUP_CLAUSE);

        result_list = lappend(result_list, tle);
    }

    return result_list;
};

void ClauseParser::freeGroupList(PGList * grouplist)
{
    PGListCell * gl;

    if (grouplist == NULL)
        return;

    foreach (gl, grouplist)
    {
        PGNode * node = (PGNode *)lfirst(gl);
        if (node == NULL)
            continue;
        if (IsA(node, GroupingClause))
        {
            GroupingClause * gc = (GroupingClause *)node;
            freeGroupList(gc->groupsets);
            pfree(gc);
        }

        else
        {
            pfree(node);
        }
    }

    pfree(grouplist);
};

PGList * ClauseParser::reorderGroupList(PGList * grouplist)
{
    PGList * result = NIL;
    PGListCell * gl;
    PGList * sub_list = NIL;

    foreach (gl, grouplist)
    {
        PGNode * node = (PGNode *)lfirst(gl);

        if (node == NULL)
        {
            /* Append an empty set. */
            result = list_concat(result, list_make1(NIL));
        }

        else if (IsA(node, GroupingClause))
        {
            GroupingClause * gc = (GroupingClause *)node;
            GroupingClause * new_gc = makeNode(GroupingClause);

            new_gc->groupType = gc->groupType;
            new_gc->groupsets = reorderGroupList(gc->groupsets);

            sub_list = lappend(sub_list, new_gc);
        }
        else
        {
            PGNode * new_node = (PGNode *)copyObject(node);
            result = lappend(result, new_node);
        }
    }

    result = list_concat(result, sub_list);
    return result;
};

PGList * ClauseParser::transformGroupClause(PGParseState * pstate,
        PGList * grouplist, PGList ** targetlist,
        PGList * sortClause, bool useSQL99)
{
    PGList * result = NIL;
    PGList * tle_list = NIL;
    PGListCell * l;
    PGList * reorder_grouplist = NIL;

    /* Preprocess the grouping clause, lookup TLEs */
    foreach (l, grouplist)
    {
        PGList * tl;
        PGListCell * tl_cell;
        PGTargetEntry * tle;
        Oid restype;
        PGNode * node;

        node = (PGNode *)lfirst(l);
        tl = findListTargetlistEntries(pstate, node, targetlist, false, false, useSQL99);

        /* CDB: Cursor position not available for errors below this point. */
        pstate->p_breadcrumb.node = NULL;

        foreach (tl_cell, tl)
        {
            tle = (PGTargetEntry *)lfirst(tl_cell);

            /* if tlist item is an UNKNOWN literal, change it to TEXT */
            restype = exprType((PGNode *)tle->expr);

            if (restype == UNKNOWNOID)
                tle->expr
                    = (PGExpr *)coerce_parser_ptr->coerce_type(pstate, (PGNode *)tle->expr, restype, TEXTOID, -1, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);

            /*
			 * The tle_list will be used to match with the ORDER by element below.
			 * We only append the tle to tle_list when node is not a
			 * GroupingClause or tle->expr is not a RowExpr.
			 */
            if (node != NULL && !IsA(node, GroupingClause) && !IsA(tle->expr, PGRowExpr))
                tle_list = lappend(tle_list, tle);
        }
    }

    /* create first group clauses based on sort clauses */
    PGList * tle_list_remainder = NIL;
    result = create_group_clause(tle_list, *targetlist, sortClause, &tle_list_remainder);

    /*
	 * Now add all remaining elements of the GROUP BY list to the result list.
	 * The result list is a list of GroupClauses and/or GroupingClauses.
	 * In each grouping set, all GroupClauses will appear in front of
	 * GroupingClauses. See the following GROUP BY clause:
	 *
	 *   GROUP BY ROLLUP(b,c),a, ROLLUP(e,d)
	 *
	 * the result list can be roughly represented as follows.
	 *
	 *    GroupClause(a),
	 *    GroupingClause(ROLLUP,groupsets(GroupClause(b),GroupClause(c))),
	 *    GroupingClause(CUBE,groupsets(GroupClause(e),GroupClause(d)))
	 *
	 *   XXX: the above transformation doesn't make sense -gs
	 */
    reorder_grouplist = reorderGroupList(grouplist);

    foreach (l, reorder_grouplist)
    {
        PGNode * node = (PGNode *)lfirst(l);
        PGTargetEntry * tle;
        GroupClause * gc;
        Oid sort_op;

        if (node == NULL) /* the empty grouping set */
            result = list_concat(result, list_make1(NIL));

        else if (IsA(node, GroupingClause))
        {
            GroupingClause * tmp = make_grouping_clause(pstate, (GroupingClause *)node, *targetlist);
            result = lappend(result, tmp);
        }

        else if (IsA(node, PGRowExpr))
        {
            /* The top level RowExprs are handled differently with other expressions.
			 * We convert each argument into GroupClause and append them
			 * one by one into 'result' list.
			 *
			 * Note that RowExprs are not added into the final targetlist.
			 */
            result = transformRowExprToGroupClauses(pstate, (PGRowExpr *)node, result, *targetlist);
        }

        else
        {
            if (useSQL99)
                tle = findTargetlistEntrySQL99(pstate, node, targetlist);
            else
                tle = findTargetlistEntrySQL92(pstate, node, targetlist, GROUP_CLAUSE);

            sort_op = oper_parser_ptr->ordering_oper_opid(exprType((PGNode *)tle->expr));

            /* avoid making duplicate expression entries */
            if (targetIsInSortGroupList(tle, sort_op, result))
                continue;
            
            gc = make_group_clause(tle, *targetlist, sort_op);
            result = lappend(result, gc);
        }
    }

    /* We're doing extended grouping for both ordinary grouping and grouping
	 * extensions.
	 */
    {
        PGList * grp_tles = NIL;
        PGListCell * lc;
        grouping_rewrite_ctx ctx;

        /* Find all unique target entries appeared in reorder_grouplist. */
        foreach (lc, reorder_grouplist)
        {
            grp_tles = list_concat_unique(grp_tles, findListTargetlistEntries(pstate, lfirst(lc), targetlist, false, true, useSQL99));
        }

        /* CDB: Cursor position not available for errors below this point. */
        pstate->p_breadcrumb.node = NULL;

        /*
		 * For each GROUPING function, check if its argument(s) appear in the
		 * GROUP BY clause. We also set ngrpcols, nargs and grpargs values for
		 * each GROUPING function here. These values are used together with
		 * GROUPING_ID to calculate the final value for each GROUPING function
		 * in the executor.
		 */

        ctx.grp_tles = grp_tles;
        ctx.pstate = pstate;
        pg_expression_tree_walker((PGNode *)*targetlist, pg_grouping_rewrite_walker, (void *)&ctx);

        /*
		 * The expression might be present in a window clause as well
		 * so process those.
		 */
        pg_expression_tree_walker((PGNode *)pstate->p_win_clauses, pg_grouping_rewrite_walker, (void *)&ctx);

        /*
		 * The expression might be present in the having clause as well.
		 */
        pg_expression_tree_walker(pstate->having_qual, pg_grouping_rewrite_walker, (void *)&ctx);
    }

    list_free(tle_list);
    list_free(tle_list_remainder);
    freeGroupList(reorder_grouplist);

    return result;
};

PGList * ClauseParser::transformScatterClause(PGParseState * pstate,
        PGList * scatterlist, PGList ** targetlist)
{
    PGList * outlist = NIL;
    PGListCell * olitem;

    /* Special case handling for SCATTER RANDOMLY */
    if (list_length(scatterlist) == 1 && linitial(scatterlist) == NULL)
        return list_make1(NULL);

    /* preprocess the scatter clause, lookup TLEs */
    foreach (olitem, scatterlist)
    {
        PGNode * node = lfirst(olitem);
        PGTargetEntry * tle;

        tle = findTargetlistEntrySQL99(pstate, node, targetlist);

        /* coerce unknown to text */
        if (exprType((PGNode *)tle->expr) == UNKNOWNOID)
        {
            tle->expr
                = (PGExpr *)coerce_parser_ptr->coerce_type(pstate, (PGNode *)tle->expr, UNKNOWNOID, TEXTOID, -1, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
        }

        outlist = lappend(outlist, tle->expr);
    }
    return outlist;
};

void ClauseParser::transformWindowClause(PGParseState * pstate, PGQuery * qry)
{
    PGListCell * w;
    PGList * winout = NIL;
    PGList * winin = pstate->p_win_clauses;
    Index clauseno = -1;

    /*
	 * We have two lists of window specs: one in the ParseState -- put there
	 * when we find the OVER(...) clause in the targetlist and the other
	 * is windowClause, a list of named window clauses. So, we concatenate
	 * them together.
	 *
	 * Note that we're careful place those found in the target list at
	 * the end because the spec might refer to a named clause and we'll
	 * after to know about those first.
	 */

    foreach (w, winin)
    {
        WindowSpec * ws = lfirst(w);
        WindowSpec * newspec = makeNode(WindowSpec);
        PGListCell * tmp;
        bool found = false;

        clauseno++;

        /* Include this WindowSpec's location in error messages. */
        pstate->p_breadcrumb.node = (PGNode *)ws;

        if (checkExprHasWindFuncs((PGNode *)ws))
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("cannot use window function in a window "
                        "specification"),
                 errOmitLocation(true)));
        }
        /*
		 * Loop through those clauses we've already processed to
		 * a) check that the name passed in is not already taken and
		 * b) look up the parent window spec.
		 *
		 * This is obvious O(n^2) but n is small.
		 */
        if (ws->parent || ws->name)
        {
            /*
			 * Firstly, check that the parent is not a reference to this
			 * window specification.
			 */
            if (ws->parent && ws->name && strcmp(ws->parent, ws->name) == 0)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("window \"%s\" cannot reference itself", ws->name),
                     errOmitLocation(true)));

            foreach (tmp, winout)
            {
                WindowSpec * ws2 = lfirst(tmp);

                /* Only check if the name exists if wc->name is not NULL */
                if (ws->name != NULL && strcmp(ws2->name, ws->name) == 0)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg(
                             "window name \"%s\" occurs more than once "
                             "in WINDOW clause",
                             ws->name),
                         errOmitLocation(true)));

                /*
				 * If this spec has a parent reference, we need to test that
				 * the following rules are met. Given the following:
				 *
				 * 		OVER (myspec ...) ... WINDOW myspec AS (...)
				 *
				 * the OVER clause cannot have a partitioning clause; only
				 * the OVER clause or myspec can have an ORDER clause; myspec
				 * cannot have a framing clause.
				 */

                /*
				 * XXX: these errors could apply to any number of clauses in the
				 * query and may be considered ambiguous by the user. Perhaps a
				 * location (see FuncCall) would help?
				 */
                if (ws->parent && ws2->name && strcmp(ws->parent, ws2->name) == 0)
                {
                    found = true;
                    if (ws->partition != NIL)
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("PARTITION BY not allowed when "
                                    "an existing window name is specified"),
                             errOmitLocation(true)));

                    if (ws->order != NIL && ws2->order != NIL)
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("conflicting ORDER BY clauses in window "
                                    "specification"),
                             errOmitLocation(true)));

                    /*
					 * We only want to disallow the specification of a
					 * framing clause when the target list form is like:
					 *
					 *  foo() OVER (w1 ORDER BY baz) ...
					 */
                    if (!(ws->partition == NIL && ws->order == NIL && ws->name == NULL) && ws2->frame)
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg(
                                 "window specification \"%s\" cannot have "
                                 "a framing clause",
                                 ws2->name),
                             errhint("Window specifications which are "
                                     "referenced by other window "
                                     "specifications cannot have framing "
                                     "clauses"),
                             errOmitLocation(true),
                             parser_errposition(pstate, ws2->location)));

                    /*
					 * The specifications are valid so just copy the details
					 * from the parent spec.
					 */
                    newspec->parent = ws2->name;
                    /* XXX: some parameters might not be processed! */
                    newspec->partition = copyObject(ws2->partition);

                    if (ws->order == NIL && ws2->order != NIL)
                        newspec->order = copyObject(ws2->order);

                    newspec->frame = copyObject(ws2->frame);
                }
            }

            if (!found && ws->parent)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("window specification \"%s\" not found", ws->parent), errOmitLocation(true)));
        }

        newspec->name = ws->name;
        newspec->location = ws->location;

        /*
		 * Process partition, if one is defined and if it isn't already
		 * in newspec.
		 */
        if (!newspec->partition && ws->partition)
        {
            newspec->partition
                = transformSortClause(pstate, ws->partition, &qry->targetList, true /* fix unknowns */, false /* use SQL92 rules */);
        }
        /* order is just like partition */
        if (ws->order || newspec->order)
        {
            /*
			 * Only do this if it came from the new definition
			 */
            if (ws->order != NIL && newspec->order == NIL)
            {
                newspec->order
                    = transformSortClause(pstate, ws->order, &qry->targetList, true /* fix unknowns */, false /* use SQL92 rules */);
            }
        }

        /* Refresh our breadcrumb in case transformSortClause stepped on it. */
        pstate->p_breadcrumb.node = (PGNode *)ws;

        /*
		 * Finally, process the framing clause. parseProcessWindFunc() will
		 * have picked up window functions that do not support framing.
		 *
		 * What we do need to do is the following:
		 * - If BETWEEN has been specified, the trailing bound is not
		 *   UNBOUNDED FOLLOWING; the leading bound is not UNBOUNDED
		 *   PRECEDING; if the first bound specifies CURRENT ROW, the
		 *   second bound shall not specify a PRECEDING bound; if the
		 *   first bound specifies a FOLLOWING bound, the second bound
		 *   shall not specify a PRECEDING or CURRENT ROW bound.
		 *
		 * - If the user did not specify BETWEEN, the bound is assumed to be
		 *   a trailing bound and the leading bound is set to CURRENT ROW.
		 *   We're careful not to set is_between here because the user did not
		 *   specify it.
		 *
		 * - If RANGE is specified: the ORDER BY clause of the window spec
		 *   may specify only one column; the type of that column must support
		 *   +/- <integer> operations and must be merge-joinable.
		 */
        if (ws->frame)
        {
            /* with that out of the way, we may proceed */
            WindowFrame * nf = copyObject(ws->frame);

            /*
			 * Framing is only supported on specifications with an ordering
			 * clause.
			 */
            if (!ws->order && !newspec->order)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("window specifications with a framing clause "
                            "must have an ORDER BY clause"),
                     errOmitLocation(true)));

            if (nf->is_between)
            {
                Assert(PointerIsValid(nf->trail));
                Assert(PointerIsValid(nf->lead));

                if (nf->trail->kind == WINDOW_UNBOUND_FOLLOWING)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("First bound of BETWEEN clause in window "
                                 "specification cannot be UNBOUNDED FOLLOWING"),
                         errOmitLocation(true)));
                if (nf->lead->kind == WINDOW_UNBOUND_PRECEDING)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("Second bound of BETWEEN clause in window "
                                 "specification cannot be UNBOUNDED PRECEDING"),
                         errOmitLocation(true)));
                if (nf->trail->kind == WINDOW_CURRENT_ROW
                    && (nf->lead->kind == WINDOW_BOUND_PRECEDING || nf->lead->kind == WINDOW_UNBOUND_PRECEDING))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("Second bound cannot be PRECEDING "
                                 "when first bound is CURRENT ROW"),
                         errOmitLocation(true)));
                if ((nf->trail->kind == WINDOW_BOUND_FOLLOWING || nf->trail->kind == WINDOW_UNBOUND_FOLLOWING)
                    && !(nf->lead->kind == WINDOW_BOUND_FOLLOWING || nf->lead->kind == WINDOW_UNBOUND_FOLLOWING))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("Second bound must be FOLLOWING if first "
                                 "bound is FOLLOWING"),
                         errOmitLocation(true)));
            }
            else
            {
                /*
				 * If only a single bound has been specified, set the
				 * leading bound to CURRENT ROW
				 */
                WindowFrameEdge * e = makeNode(WindowFrameEdge);

                Assert(!PointerIsValid(nf->lead));

                e->kind = WINDOW_CURRENT_ROW;
                nf->lead = e;
            }

            transformWindowFrameEdge(pstate, nf->trail, newspec, qry, nf->is_rows);
            transformWindowFrameEdge(pstate, nf->lead, newspec, qry, nf->is_rows);
            newspec->frame = nf;
        }

        /* finally, check function restriction with this spec. */
        winref_checkspec(pstate, qry, clauseno, PointerIsValid(newspec->order), PointerIsValid(newspec->frame));

        winout = lappend(winout, newspec);
    }

    /* If there are no window functions in the targetlist,
	 * forget the window clause.
	 */
    if (!pstate->p_hasWindFuncs)
    {
        pstate->p_win_clauses = NIL;
        qry->windowClause = NIL;
    }
    else
    {
        qry->windowClause = winout;
    }
};

PGList * ClauseParser::transformDistinctToGroupBy(PGParseState * pstate,
    PGList ** targetlist,
    PGList ** sortClause,
    PGList ** groupClause)
{
    PGList * group_tlist = list_copy(*targetlist);

    /*
	 * create first group clauses based on matching sort clauses, if any
	 */
    PGList * group_tlist_remainder = NIL;
    PGList * group_clause_list = create_group_clause(group_tlist, *targetlist, *sortClause, &group_tlist_remainder);

    if (list_length(group_tlist_remainder) > 0)
    {
        /*
		 * append remaining group clauses to the end of group clause list
		 */
        PGListCell * lc = NULL;

        foreach (lc, group_tlist_remainder)
        {
            PGTargetEntry * tle = (PGTargetEntry *)lfirst(lc);
            if (!tle->resjunk)
            {
                PGSortBy sortby;

                sortby.type = T_PGSortBy;
                sortby.sortby_dir = PG_SORTBY_DEFAULT;
                sortby.sortby_nulls = PG_SORTBY_NULLS_DEFAULT;
                sortby.useOp = NIL;
                sortby.location = -1;
                sortby.node = (PGNode *)tle->expr;
                group_clause_list = addTargetToSortList(pstate, tle, group_clause_list, *targetlist, &sortby, true);
            }
        }
    }

    *groupClause = group_clause_list;

    list_free(group_tlist);
    list_free(group_tlist_remainder);

    /*
	 * return empty distinct list, since we have created a grouping clause to do the job
	 */
    return NIL;
};

PGList * ClauseParser::transformDistinctOnClause(PGParseState * pstate,
    PGList * distinctlist,
    PGList ** targetlist,
    PGList * sortClause)
{
    PGList * result = NIL;
    PGList * sortgrouprefs = NIL;
    bool skipped_sortitem;
    PGListCell * lc;
    PGListCell * lc2;

    /*
	 * Add all the DISTINCT ON expressions to the tlist (if not already
	 * present, they are added as resjunk items).  Assign sortgroupref numbers
	 * to them, and make a list of these numbers.  (NB: we rely below on the
	 * sortgrouprefs list being one-for-one with the original distinctlist.
	 * Also notice that we could have duplicate DISTINCT ON expressions and
	 * hence duplicate entries in sortgrouprefs.)
	 */
    foreach (lc, distinctlist)
    {
        PGNode * dexpr = (PGNode *)lfirst(lc);
        int sortgroupref;
        PGTargetEntry * tle;

        tle = findTargetlistEntrySQL92(pstate, dexpr, targetlist, EXPR_KIND_DISTINCT_ON);
        sortgroupref = assignSortGroupRef(tle, *targetlist);
        sortgrouprefs = lappend_int(sortgrouprefs, sortgroupref);
    }

    /*
	 * If the user writes both DISTINCT ON and ORDER BY, adopt the sorting
	 * semantics from ORDER BY items that match DISTINCT ON items, and also
	 * adopt their column sort order.  We insist that the distinctClause and
	 * sortClause match, so throw error if we find the need to add any more
	 * distinctClause items after we've skipped an ORDER BY item that wasn't
	 * in DISTINCT ON.
	 */
    skipped_sortitem = false;
    foreach (lc, sortClause)
    {
        PGSortGroupClause * scl = (PGSortGroupClause *)lfirst(lc);

        if (list_member_int(sortgrouprefs, scl->tleSortGroupRef))
        {
            if (skipped_sortitem)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
                     parser_errposition(pstate, get_matching_location(scl->tleSortGroupRef, sortgrouprefs, distinctlist))));
            else
                result = lappend(result, copyObject(scl));
        }
        else
            skipped_sortitem = true;
    }

    /*
	 * Now add any remaining DISTINCT ON items, using default sort/group
	 * semantics for their data types.  (Note: this is pretty questionable; if
	 * the ORDER BY list doesn't include all the DISTINCT ON items and more
	 * besides, you certainly aren't using DISTINCT ON in the intended way,
	 * and you probably aren't going to get consistent results.  It might be
	 * better to throw an error or warning here.  But historically we've
	 * allowed it, so keep doing so.)
	 */
    forboth(lc, distinctlist, lc2, sortgrouprefs)
    {
        PGNode * dexpr = (PGNode *)lfirst(lc);
        int sortgroupref = lfirst_int(lc2);
        PGTargetEntry * tle = get_sortgroupref_tle(sortgroupref, *targetlist);

        if (targetIsInSortList(tle, InvalidOid, result))
            continue; /* already in list (with some semantics) */
        if (skipped_sortitem)
            ereport(
                ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                 errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
                 parser_errposition(pstate, exprLocation(dexpr))));
        result = addTargetToGroupList(pstate, tle, result, *targetlist, exprLocation(dexpr), true);
    }

    /*
	 * An empty result list is impossible here because of grammar
	 * restrictions.
	 */
    Assert(result != NIL);

    return result;
};

PGList * ClauseParser::transformDistinctClause(PGParseState * pstate,
        PGList * distinctlist, PGList ** targetlist,
        PGList ** sortClause, PGList ** groupClause)
{
    PGList * result = NIL;
    PGListCell * slitem;
    PGListCell * dlitem;

    /* No work if there was no DISTINCT clause */
    if (distinctlist == NIL)
        return NIL;

    if (linitial(distinctlist) == NULL)
    {
        /* We had SELECT DISTINCT */

        if (!pstate->p_hasAggs && !pstate->p_hasWindFuncs && *groupClause == NIL)
        {
            /*
			 * MPP-15040
			 * turn distinct clause into grouping clause to make both sort-based
			 * and hash-based grouping implementations viable plan options
			 */

            return transformDistinctToGroupBy(pstate, targetlist, sortClause, groupClause);
        }

        /*
		 * All non-resjunk elements from target list that are not already in
		 * the sort list should be added to it.  (We don't really care what
		 * order the DISTINCT fields are checked in, so we can leave the
		 * user's ORDER BY spec alone, and just add additional sort keys to it
		 * to ensure that all targetlist items get sorted.)
		 */
        *sortClause = addAllTargetsToSortList(pstate, *sortClause, *targetlist, true);

        /*
		 * Now, DISTINCT list consists of all non-resjunk sortlist items.
		 * Actually, all the sortlist items had better be non-resjunk!
		 * Otherwise, user wrote SELECT DISTINCT with an ORDER BY item that
		 * does not appear anywhere in the SELECT targetlist, and we can't
		 * implement that with only one sorting pass...
		 */
        foreach (slitem, *sortClause)
        {
            SortClause * scl = (SortClause *)lfirst(slitem);
            PGTargetEntry * tle = get_sortgroupclause_tle(scl, *targetlist);

            if (tle->resjunk)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
                     errOmitLocation(true)));
            else
                result = lappend(result, copyObject(scl));
        }
    }
    else
    {
        /* We had SELECT DISTINCT ON (expr, ...) */

        /*
		 * If the user writes both DISTINCT ON and ORDER BY, then the two
		 * expression lists must match (until one or the other runs out).
		 * Otherwise the ORDER BY requires a different sort order than the
		 * DISTINCT does, and we can't implement that with only one sort pass
		 * (and if we do two passes, the results will be rather
		 * unpredictable). However, it's OK to have more DISTINCT ON
		 * expressions than ORDER BY expressions; we can just add the extra
		 * DISTINCT values to the sort list, much as we did above for ordinary
		 * DISTINCT fields.
		 *
		 * Actually, it'd be OK for the common prefixes of the two lists to
		 * match in any order, but implementing that check seems like more
		 * trouble than it's worth.
		 */
        PGListCell * nextsortlist = list_head(*sortClause);

        foreach (dlitem, distinctlist)
        {
            PGTargetEntry * tle;

            tle = findTargetlistEntrySQL92(pstate, lfirst(dlitem), targetlist, DISTINCT_ON_CLAUSE);

            if (nextsortlist != NULL)
            {
                SortClause * scl = (SortClause *)lfirst(nextsortlist);

                if (tle->ressortgroupref != scl->tleSortGroupRef)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                         errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
                         errOmitLocation(true)));
                result = lappend(result, copyObject(scl));
                nextsortlist = lnext(nextsortlist);
            }
            else
            {
                *sortClause = addTargetToSortList(pstate, tle, *sortClause, *targetlist, SORTBY_ASC, NIL, true);

                /*
				 * Probably, the tle should always have been added at the end
				 * of the sort list ... but search to be safe.
				 */
                foreach (slitem, *sortClause)
                {
                    SortClause * scl = (SortClause *)lfirst(slitem);

                    if (tle->ressortgroupref == scl->tleSortGroupRef)
                    {
                        result = lappend(result, copyObject(scl));
                        break;
                    }
                }
                if (slitem == NULL) /* should not happen */
                    elog(ERROR, "failed to add DISTINCT ON clause to target list");
            }
        }
    }

    return result;
};

PGNode * ClauseParser::transformLimitClause(PGParseState * pstate,
        PGNode * clause, const char * constructName)
{
    PGNode * qual;

    if (clause == NULL)
        return NULL;

    qual = expr_parser_ptr->transformExpr(pstate, clause);

    qual = coerce_parser_ptr->coerce_to_bigint(pstate, qual, constructName);

    /*
	 * LIMIT can't refer to any vars or aggregates of the current query; we
	 * don't allow subselects either (though that case would at least be
	 * sensible)
	 */
    if (contain_vars_of_level(qual, 0))
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
             /* translator: %s is name of a SQL construct, eg LIMIT */
             errmsg("argument of %s must not contain variables", constructName)));
    }
    if (checkExprHasAggs(qual))
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
             /* translator: %s is name of a SQL construct, eg LIMIT */
             errmsg("argument of %s must not contain aggregates", constructName)));
    }
    if (contain_subplans(qual))
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             /* translator: %s is name of a SQL construct, eg LIMIT */
             errmsg("argument of %s must not contain subqueries", constructName)));
    }

    return qual;
};

}
