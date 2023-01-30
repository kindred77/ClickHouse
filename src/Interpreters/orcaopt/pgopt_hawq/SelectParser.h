#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/CTEParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/ClauseParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/AggParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/TargetParser.h>

namespace DB
{

class SelectParser
{
private:
    CTEParser cte_parser;
    ClauseParser clause_parser;
    AggParser agg_parser;
    TargetParser target_parser;
public:
	explicit SelectParser();

    duckdb_libpgquery::PGQuery * transformSelectStmt(PGParseState * pstate,
        duckdb_libpgquery::PGSelectStmt * stmt);

    duckdb_libpgquery::PGQuery * transformStmt(
        PGParseState * pstate,
        duckdb_libpgquery::PGNode * parseTree,
        duckdb_libpgquery::PGList ** extras_before,
        duckdb_libpgquery::PGList ** extras_after);

    duckdb_libpgquery::PGList * do_parse_analyze(duckdb_libpgquery::PGNode * parseTree,
        PGParseState * pstate);

    duckdb_libpgquery::PGFromExpr *makeFromExpr(duckdb_libpgquery::PGList *fromlist,
        duckdb_libpgquery::PGNode *quals);

    duckdb_libpgquery::PGList *
    parse_analyze(duckdb_libpgquery::PGNode * parseTree,
        const char * sourceText, Oid * paramTypes, int numParams);
};

}