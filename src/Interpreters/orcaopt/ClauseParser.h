#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/ExprParser.h>
#include <Interpreters/orcaopt/TargetParser.h>

#define ORDER_CLAUSE 0
#define GROUP_CLAUSE 1
#define DISTINCT_ON_CLAUSE 2

namespace DB
{

typedef struct pg_grouping_rewrite_ctx
{
    duckdb_libpgquery::PGList * grp_tles;
    PGParseState * pstate;
} pg_grouping_rewrite_ctx;

bool pg_grouping_rewrite_walker(duckdb_libpgquery::PGNode * node, void * context)
{
    using duckdb_libpgquery::PGListCell;
    using duckdb_libpgquery::PGList;
    using duckdb_libpgquery::PGNode;
    using duckdb_libpgquery::PGTargetEntry;
    using duckdb_libpgquery::PGRangeTblEntry;
    using duckdb_libpgquery::PGVar;
    using duckdb_libpgquery::PGRowExpr;

    using duckdb_libpgquery::PGAExpr;
    using duckdb_libpgquery::PGColumnRef;
    using duckdb_libpgquery::PGAConst;
    using duckdb_libpgquery::PGTypeCast;
    using duckdb_libpgquery::PGGroupingFunc;
    using duckdb_libpgquery::PGSortBy;
    using duckdb_libpgquery::T_PGAExpr;
    using duckdb_libpgquery::T_PGColumnRef;
    using duckdb_libpgquery::T_PGAConst;
    using duckdb_libpgquery::T_PGTypeCast;
    using duckdb_libpgquery::T_PGGroupingFunc;
    using duckdb_libpgquery::T_PGVar;
    using duckdb_libpgquery::T_PGRowExpr;
    using duckdb_libpgquery::T_PGSortBy;

    using duckdb_libpgquery::ereport;
    using duckdb_libpgquery::errcode;
    using duckdb_libpgquery::errmsg;
    using duckdb_libpgquery::makeInteger;

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

class ClauseParser
{
private:
  RelationParserPtr relation_parser_ptr;
  SelectParserPtr select_parser_ptr;
  CoerceParserPtr coerce_parser_ptr;
  ExprParserPtr expr_parser_ptr;
  TargetParserPtr target_parser_ptr;
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

    duckdb_libpgquery::PGTargetEntry * findTargetlistEntrySQL92(PGParseState * pstate,
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
