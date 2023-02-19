#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{
class ClauseParser;
class TargetParser;
class CoerceParser;
class NodeParser;
class RelationParser;
class CTEParser;
class FuncParser;
class AggParser;
using ClauseParserPtr = std::unique_ptr<ClauseParser>;
using TargetParserPtr = std::unique_ptr<TargetParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using RelationParserPtr = std::unique_ptr<RelationParser>;
using CTEParserPtr = std::unique_ptr<CTEParser>;
using FuncParserPtr = std::unique_ptr<FuncParser>;
using AggParserPtr = std::unique_ptr<AggParser>;

class SelectParser
{
private:
    ClauseParserPtr clause_parser;
    TargetParserPtr target_parser;
    CoerceParserPtr coerce_parser;
    NodeParserPtr node_parser;
    RelationParserPtr relation_parser;
    CTEParserPtr cte_parser;
    //CollationParserPtr collation_parser;
    AggParserPtr agg_parser;
    FuncParserPtr func_parser;
public:
	explicit SelectParser();

    void
    markTargetListOrigin(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGVar *var, int levelsup);

    void
    markTargetListOrigins(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);

    duckdb_libpgquery::PGQuery *
    transformStmt(PGParseState *pstate, duckdb_libpgquery::PGNode *parseTree);

    duckdb_libpgquery::PGQuery * transformGroupedWindows(PGParseState * pstate,
        duckdb_libpgquery::PGQuery * qry);

    duckdb_libpgquery::PGQuery *
    transformSelectStmt(PGParseState *pstate, duckdb_libpgquery::PGSelectStmt *stmt);

    duckdb_libpgquery::PGQuery *
    parse_sub_analyze(duckdb_libpgquery::PGNode *parseTree, PGParseState *parentParseState,
				  duckdb_libpgquery::PGCommonTableExpr *parentCTE,
				  duckdb_libpgquery::PGLockingClause *lockclause_from_parent);
};

}
