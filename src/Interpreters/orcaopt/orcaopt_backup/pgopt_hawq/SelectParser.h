#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class CTEParser;
class ClauseParser;
class AggParser;
class TargetParser;
class NodeParser;
class CoerceParser;
using CTEParserPtr = std::unique_ptr<CTEParser>;
using ClauseParserPtr = std::unique_ptr<ClauseParser>;
using AggParserPtr = std::unique_ptr<AggParser>;
using TargetParserPtr = std::unique_ptr<TargetParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;

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
        duckdb_libpgquery::PGNode * parseTree);

    duckdb_libpgquery::PGList * do_parse_analyze(duckdb_libpgquery::PGNode * parseTree,
        PGParseState * pstate);

    duckdb_libpgquery::PGFromExpr *makeFromExpr(duckdb_libpgquery::PGList *fromlist,
        duckdb_libpgquery::PGNode *quals);

    duckdb_libpgquery::PGList *
    parse_analyze(duckdb_libpgquery::PGNode * parseTree,
        const char * sourceText, Oid * paramTypes, int numParams);

    duckdb_libpgquery::PGQuery *
    parse_sub_analyze(duckdb_libpgquery::PGNode * parseTree, PGParseState * parentParseState,
        duckdb_libpgquery::PGCommonTableExpr * parentCTE, duckdb_libpgquery::PGLockingClause * lockclause_from_parent);
};

}
