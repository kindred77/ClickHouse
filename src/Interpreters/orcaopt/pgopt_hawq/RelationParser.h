#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

#include <Storages/IStorage.h>

#include <optional>

namespace DB
{

class RelationParser
{
private:

public:
    explicit RelationParser();

    int RTERangeTablePosn(PGParseState * pstate, duckdb_libpgquery::PGRangeTblEntry * rte, int * sublevels_up);

    duckdb_libpgquery::PGRangeTblEntry * GetRTEByRangeTablePosn(PGParseState * pstate, int varno, int sublevels_up);

    duckdb_libpgquery::PGNode * scanRTEForColumn(PGParseState * pstate,
      duckdb_libpgquery::PGRangeTblEntry * rte, char * colname, int location);

    duckdb_libpgquery::PGNode * colNameToVar(PGParseState * pstate, char * colname, bool localonly, int location);

    duckdb_libpgquery::PGTargetEntry * get_tle_by_resno(duckdb_libpgquery::PGList * tlist,
		PGAttrNumber resno);

    void expandRTE(
        duckdb_libpgquery::PGRangeTblEntry * rte,
        int rtindex,
        int sublevels_up,
        int location,
        bool include_dropped,
        duckdb_libpgquery::PGList ** colnames,
        duckdb_libpgquery::PGList ** colvars);

    void checkNameSpaceConflicts(PGParseState * pstate, duckdb_libpgquery::PGList * namespace1, duckdb_libpgquery::PGList * namespace2);

    duckdb_libpgquery::PGCommonTableExpr * scanNameSpaceForCTE(PGParseState * pstate, const char * refname, Index * ctelevelsup);

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntryForCTE(PGParseState * pstate,
		duckdb_libpgquery::PGCommonTableExpr * cte, Index levelsup,
		duckdb_libpgquery::PGRangeVar * rangeVar, bool inFromCl);

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntry(PGParseState * pstate,
		duckdb_libpgquery::PGRangeVar * relation, duckdb_libpgquery::PGAlias * alias,
		bool inh, bool inFromCl);

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntryForSubquery(PGParseState * pstate,
		duckdb_libpgquery::PGQuery * subquery, duckdb_libpgquery::PGAlias * alias, bool inFromCl);
};

}