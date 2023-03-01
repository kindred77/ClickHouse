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
class RelationProvider;
using ClauseParserPtr = std::shared_ptr<ClauseParser>;
using TargetParserPtr = std::shared_ptr<TargetParser>;
using CoerceParserPtr = std::shared_ptr<CoerceParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using RelationParserPtr = std::shared_ptr<RelationParser>;
using CTEParserPtr = std::shared_ptr<CTEParser>;
using FuncParserPtr = std::shared_ptr<FuncParser>;
using AggParserPtr = std::shared_ptr<AggParser>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;

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
    RelationProviderPtr relation_provider;
public:
	explicit SelectParser();

    void
    markTargetListOrigin(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGVar *var, int levelsup);

    void
    markTargetListOrigins(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);

    duckdb_libpgquery::PGQuery *
    transformStmt(PGParseState *pstate, duckdb_libpgquery::PGNode *parseTree);

    duckdb_libpgquery::PGNode * map_sgr_mutator(duckdb_libpgquery::PGNode * node, void * context);

    void init_grouped_window_context(grouped_window_ctx * ctx, duckdb_libpgquery::PGQuery * qry);

    void discard_grouped_window_context(grouped_window_ctx * ctx);

    duckdb_libpgquery::PGQuery * transformGroupedWindows(PGParseState * pstate,
        duckdb_libpgquery::PGQuery * qry);

    duckdb_libpgquery::PGQuery *
    transformSelectStmt(PGParseState *pstate, duckdb_libpgquery::PGSelectStmt *stmt);

    duckdb_libpgquery::PGQuery *
    parse_sub_analyze(duckdb_libpgquery::PGNode *parseTree, PGParseState *parentParseState,
				  duckdb_libpgquery::PGCommonTableExpr *parentCTE,
				  duckdb_libpgquery::PGLockingClause *lockclause_from_parent);
    
    duckdb_libpgquery::PGAlias * make_replacement_alias(duckdb_libpgquery::PGQuery *qry, const char *aname);
};

}
