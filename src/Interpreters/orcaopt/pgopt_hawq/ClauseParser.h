#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/SelectParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/ExprParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/TargetParser.h>

#define ORDER_CLAUSE 0
#define GROUP_CLAUSE 1
#define DISTINCT_ON_CLAUSE 2

namespace DB
{

typedef struct pg_grouping_rewrite_ctx
{
    PGList * grp_tles;
    PGParseState * pstate;
} pg_grouping_rewrite_ctx;

bool pg_grouping_rewrite_walker(duckdb_libpgquery::PGNode * node, void * context)
{
    pg_grouping_rewrite_ctx * ctx = (pg_grouping_rewrite_ctx *)context;

    if (node == NULL)
        return false;

    if (IsA(node, duckdb_libpgquery::PGAConst))
    {
        return false;
    }
    else if (IsA(node, duckdb_libpgquery::PGAExpr))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }
    else if (IsA(node, duckdb_libpgquery::PGColumnRef))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }
    else if (IsA(node, duckdb_libpgquery::PGTypeCast))
    {
        return false;
    }
    else if (IsA(node, duckdb_libpgquery::PGGroupingFunc))
    {
        duckdb_libpgquery::PGGroupingFunc * gf = (duckdb_libpgquery::PGGroupingFunc *)node;
        duckdb_libpgquery::PGListCell * arg_lc;
        duckdb_libpgquery::PGList * newargs = NIL;

        gf->ngrpcols = list_length(ctx->grp_tles);

        /*
		 * For each argument in gf->args, find its position in grp_tles,
		 * and increment its counts. Note that this is a O(n^2) algorithm,
		 * but it should not matter that much.
		 */
        foreach (arg_lc, gf->args)
        {
            long i = 0;
            duckdb_libpgquery::PGNode * node = lfirst(arg_lc);
            duckdb_libpgquery::PGListCell * grp_lc = NULL;

            foreach (grp_lc, ctx->grp_tles)
            {
                duckdb_libpgquery::PGTargetEntry * grp_tle = (duckdb_libpgquery::PGTargetEntry *)lfirst(grp_lc);

                if (equal(grp_tle->expr, node))
                    break;
                i++;
            }

            /* Find a column not in GROUP BY clause */
            if (grp_lc == NULL)
            {
                duckdb_libpgquery::PGRangeTblEntry * rte;
                const char * attname;
                duckdb_libpgquery::PGVar * var = (duckdb_libpgquery::PGVar *)node;

                /* Do not allow expressions inside a grouping function. */
                if (IsA(node, duckdb_libpgquery::PGRowExpr))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_GROUPING_ERROR),
                         errmsg("row type can not be used inside a grouping function."),
                         errOmitLocation(true)));

                if (!IsA(node, duckdb_libpgquery::PGVar))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_GROUPING_ERROR),
                         errmsg("expression in a grouping fuction does not appear in GROUP BY."),
                         errOmitLocation(true)));

                Assert(IsA(node, duckdb_libpgquery::PGVar));
                Assert(var->varno > 0);
                Assert(var->varno <= list_length(ctx->pstate->p_rtable));

                rte = rt_fetch(var->varno, ctx->pstate->p_rtable);
                attname = get_rte_attribute_name(rte, var->varattno);

                ereport(
                    ERROR,
                    (errcode(ERRCODE_GROUPING_ERROR),
                     errmsg("column \"%s\".\"%s\" is not in GROUP BY", rte->eref->aliasname, attname),
                     errOmitLocation(true)));
            }

            newargs = lappend(newargs, makeInteger(i));
        }

        /* Free gf->args since we do not need it any more. */
        list_free_deep(gf->args);
        gf->args = newargs;
    }
    else if (IsA(node, duckdb_libpgquery::PGSortBy))
    {
        /*
		 * When WindowSpec leaves the main parser, partition and order
		 * clauses will be lists of SortBy structures. Process them here to
		 * avoid muddying up the expression_tree_walker().
		 */
        duckdb_libpgquery::PGSortBy * s = (duckdb_libpgquery::PGSortBy *)node;
        return pg_grouping_rewrite_walker(s->node, context);
    }
    return pg_expression_tree_walker(node, pg_grouping_rewrite_walker, context);
};

class ClauseParser
{
private:
  RelationParser relation_parser;
  SelectParser select_parser;
  CoerceParser coerce_parser;
  ExprParser expr_parser;
  TargetParser target_parser;
public:
	explicit ClauseParser();

    duckdb_libpgquery::PGNode * buildMergedJoinVar(PGParseState * pstate,
        duckdb_libpgquery::PGJoinType jointype, duckdb_libpgquery::PGVar * l_colvar,
        duckdb_libpgquery::PGVar * r_colvar);

    duckdb_libpgquery::PGRangeTblEntry * transformTableEntry(PGParseState * pstate, duckdb_libpgquery::PGRangeVar * r);

    duckdb_libpgquery::PGRangeTblEntry * transformRangeSubselect(PGParseState * pstate,
        duckdb_libpgquery::PGRangeSubselect * r);

    // duckdb_libpgquery::PGRangeTblEntry * transformRangeFunction(PGParseState * pstate, duckdb_libpgquery::PGRangeFunction * r);

    duckdb_libpgquery::PGNode * transformWhereClause(PGParseState * pstate,
        duckdb_libpgquery::PGNode * clause, const char * constructName);

    duckdb_libpgquery::PGNode * transformFromClauseItem(
        PGParseState * pstate,
        duckdb_libpgquery::PGNode * n,
        duckdb_libpgquery::PGRangeTblEntry ** top_rte,
        int * top_rti,
        duckdb_libpgquery::PGList ** relnamespace,
        PGRelids * containedRels);

    void
    extractRemainingColumns(duckdb_libpgquery::PGList * common_colnames, duckdb_libpgquery::PGList * src_colnames,
        duckdb_libpgquery::PGList * src_colvars, duckdb_libpgquery::PGList ** res_colnames,
        duckdb_libpgquery::PGList ** res_colvars);

    duckdb_libpgquery::PGNode * transformJoinUsingClause(PGParseState * pstate, duckdb_libpgquery::PGList * leftVars,
        duckdb_libpgquery::PGList * rightVars);

    duckdb_libpgquery::PGNode * transformJoinOnClause(
        PGParseState * pstate, duckdb_libpgquery::PGJoinExpr * j, duckdb_libpgquery::PGRangeTblEntry * l_rte,
        duckdb_libpgquery::PGRangeTblEntry * r_rte, duckdb_libpgquery::PGList * relnamespace,
        PGRelids containedRels);

    duckdb_libpgqueryetEntry * findTargetlistEntrySQL92(PGParseState * pstate,
        duckdb_libpgquery::PGNode * node, duckdb_libpgquery::PGList ** tlist, int clause);

    bool targetIsInSortGroupList(duckdb_libpgquery::PGTargetEntry * tle,
        duckdb_libpgquery::PGList * sortgroupList);

    duckdb_libpgquery::PGList * addTargetToSortList(
        PGParseState * pstate,
        duckdb_libpgquery::PGTargetEntry * tle,
        duckdb_libpgquery::PGList * sortlist,
        duckdb_libpgquery::PGList * targetlist,
        int sortby_kind,
        duckdb_libpgquery::PGList * sortby_opname,
        bool resolveUnknown);

    duckdb_libpgquery::PGList * transformSortClause(PGParseState * pstate,
        duckdb_libpgquery::PGList * orderlist, duckdb_libpgquery::PGList ** targetlist,
        bool resolveUnknown, bool useSQL99);

    duckdb_libpgquery::PGList * transformGroupClause(PGParseState * pstate,
        duckdb_libpgquery::PGList * grouplist, duckdb_libpgquery::PGList ** targetlist,
        duckdb_libpgquery::PGList * sortClause, bool useSQL99);

    duckdb_libpgquery::PGList * transformScatterClause(PGParseState * pstate,
        duckdb_libpgquery::PGList * scatterlist, duckdb_libpgquery::PGList ** targetlist);

    void transformWindowClause(PGParseState * pstate, duckdb_libpgquery::PGQuery * qry);

    duckdb_libpgquery::PGList * transformDistinctClause(PGParseState * pstate,
        duckdb_libpgquery::PGList * distinctlist, duckdb_libpgquery::PGList ** targetlist,
        duckdb_libpgquery::PGList ** sortClause, duckdb_libpgquery::PGList ** groupClause);

    duckdb_libpgquery::PGNode * transformLimitClause(PGParseState * pstate,
        duckdb_libpgquery::PGNode * clause, const char * constructName);

    duckdb_libpgquery::PGList * findListTargetlistEntries(PGParseState * pstate, duckdb_libpgquery::PGNode * node,
        duckdb_libpgquery::PGList ** tlist, bool in_grpext, bool ignore_in_grpext, bool useSQL99);

    duckdb_libpgquery::PGList * reorderGroupList(duckdb_libpgquery::PGList * grouplist);

    void freeGroupList(duckdb_libpgquery::PGList * grouplist);
};

}