#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/orcaopt/CTEParser.h>
#include <Interpreters/orcaopt/ClauseParser.h>
#include <Interpreters/orcaopt/AggParser.h>
#include <Interpreters/orcaopt/TargetParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/CoerceParser.h>

namespace DB
{

class SelectParser
{
private:
    CTEParserPtr cte_parser_ptr;
    ClauseParserPtr clause_parser_ptr;
    AggParserPtr agg_parser_ptr;
    TargetParserPtr target_parser_ptr;
    NodeParserPtr node_parser_ptr;
    CoerceParserPtr coerce_parser_ptr;
public:
	explicit SelectParser();

    void applyColumnNames(duckdb_libpgquery::PGList * dst, duckdb_libpgquery::PGList * src);

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

    duckdb_libpgquery::PGList * parse_sub_analyze(duckdb_libpgquery::PGNode * parseTree, PGParseState * parentParseState);
};
}
