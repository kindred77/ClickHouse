#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/SelectParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>

namespace DB
{

class ClauseParser
{
private:
  RelationParser relation_parser;
  SelectParser select_parser;
  CoerceParser coerce_parser;
public:
	explicit ClauseParser();

    duckdb_libpgquery::PGNode * buildMergedJoinVar(PGParseState * pstate,
        duckdb_libpgquery::PGJoinType jointype, duckdb_libpgquery::PGVar * l_colvar,
        duckdb_libpgquery::PGVar * r_colvar);

    duckdb_libpgquery::PGRangeTblEntry * transformTableEntry(PGParseState * pstate, duckdb_libpgquery::PGRangeVar * r);

    duckdb_libpgquery::PGRangeTblEntry * transformRangeSubselect(PGParseState * pstate,
        duckdb_libpgquery::PGRangeSubselect * r);

    duckdb_libpgquery::PGNode * transformWhereClause(PGParseState * pstate,
        duckdb_libpgquery::PGNode * clause, const char * constructName);

    duckdb_libpgquery::PGNode * transformFromClauseItem(
        PGParseState * pstate,
        duckdb_libpgquery::PGNode * n,
        duckdb_libpgquery::PGRangeTblEntry ** top_rte,
        int * top_rti,
        duckdb_libpgquery::PGList ** relnamespace,
        Relids * containedRels);

    bool interpretInhOption(InhOption inhOpt);

    void transformFromClause(PGParseState *pstate, duckdb_libpgquery::PGList *frmList);

    duckdb_libpgquery::PGTargetEntry * findTargetlistEntrySQL99(PGParseState * pstate,
        duckdb_libpgquery::PGNode * node, duckdb_libpgquery::PGList ** tlist);

    duckdb_libpgquery::PGTargetEntry * findTargetlistEntrySQL92(PGParseState * pstate,
        duckdb_libpgquery::PGNode * node, duckdb_libpgquery::PGList ** tlist, int clause);

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
};

}