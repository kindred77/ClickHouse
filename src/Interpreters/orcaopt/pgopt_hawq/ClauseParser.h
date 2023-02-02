#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/SelectParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/ExprParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/TargetParser.h>

namespace DB
{

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
};

}