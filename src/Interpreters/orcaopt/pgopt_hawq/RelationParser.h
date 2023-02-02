#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>

#include <Storages/IStorage.h>

#include <optional>

namespace DB
{

class RelationParser
{
private:
    CoerceParser coerce_parser;
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

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntryForCTE(
        PGParseState * pstate,
        duckdb_libpgquery::PGCommonTableExpr * cte,
        Index levelsup,
        duckdb_libpgquery::PGRangeVar * rangeVar,
        bool inFromCl);

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntry(
        PGParseState * pstate, duckdb_libpgquery::PGRangeVar * relation, duckdb_libpgquery::PGAlias * alias, bool inh, bool inFromCl);

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntryForSubquery(
        PGParseState * pstate, duckdb_libpgquery::PGQuery * subquery, duckdb_libpgquery::PGAlias * alias, bool inFromCl);

    duckdb_libpgquery::PGRangeTblEntry * scanNameSpaceForRelid(PGParseState * pstate, Oid relid);

    duckdb_libpgquery::PGRangeTblEntry * scanNameSpaceForRefname(PGParseState * pstate, const char * refname);

    duckdb_libpgquery::PGRangeTblEntry * refnameRangeTblEntryHelperSchemaQualified(
        PGParseState * pstate, Oid dboid, const char * nspname, const char * refname,
        int * sublevels_up);

    duckdb_libpgquery::PGRangeTblEntry * refnameRangeTblEntryHelper(
        PGParseState * pstate, const char * refname, Oid relId, int * sublevels_up);

    duckdb_libpgquery::PGRangeTblEntry * refnameRangeTblEntry(
        PGParseState * pstate, const char * catalogname, const char * schemaname, const char * refname, int location, int * sublevels_up);

    duckdb_libpgquery::PGRangeTblEntry * searchRangeTable(PGParseState * pstate,
        duckdb_libpgquery::PGRangeVar * relation);

    void warnAutoRange(PGParseState * pstate, duckdb_libpgquery::PGRangeVar * relation, int location);

    void addRTEtoQuery(PGParseState * pstate, duckdb_libpgquery::PGRangeTblEntry * rte,
        bool addToJoinList, bool addToRelNameSpace, bool addToVarNameSpace);

    duckdb_libpgquery::PGRangeTblEntry * addImplicitRTE(PGParseState * pstate, duckdb_libpgquery::PGRangeVar * relation,
        int location);

    duckdb_libpgquery::PGList * expandRelAttrs(PGParseState * pstate, duckdb_libpgquery::PGRangeTblEntry * rte,
        int rtindex, int sublevels_up, int location);

    void expandRelation(Oid relid, duckdb_libpgquery::PGAlias * eref, int rtindex,
        int sublevels_up, bool include_dropped, duckdb_libpgquery::PGList ** colnames,
        duckdb_libpgquery::PGList ** colvars);

    void buildRelationAliases(TupleDesc tupdesc, duckdb_libpgquery::PGAlias * alias,
        duckdb_libpgquery::PGAlias * eref);

    // duckdb_libpgquery::PGRangeTblEntry *
    // addRangeTableEntryForFunction(PGParseState * pstate, char * funcname, duckdb_libpgquery::PGNode * funcexpr,
    //     duckdb_libpgquery::PGRangeFunction * rangefunc, bool inFromCl);

    void buildScalarFunctionAlias(duckdb_libpgquery::PGNode * funcexpr, char * funcname,
        duckdb_libpgquery::PGAlias * alias, duckdb_libpgquery::PGAlias * eref);

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntryForJoin(PGParseState * pstate, duckdb_libpgquery::PGList * colnames,
        duckdb_libpgquery::PGJoinType jointype, duckdb_libpgquery::PGList * aliasvars, duckdb_libpgquery::PGAlias * alias,
        bool inFromCl);
};

}