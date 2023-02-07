#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/ClauseParser.h>
#include <Interpreters/orcaopt/pgoptnew/CoerceParser.h>
#include <Interpreters/orcaopt/pgoptnew/TargetParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/CTEParser.h>
#include <Interpreters/orcaopt/pgoptnew/CollationParser.h>
#include <Interpreters/orcaopt/pgoptnew/AggParser.h>
#include <Interpreters/orcaopt/pgoptnew/FuncParser.h>

namespace DB
{

class SelectParser
{
private:
    ClauseParser clause_parser;
    TargetParser target_parser;
    CoerceParser coerce_parser;
    NodeParser node_parser;
    RelationParser relation_parser;
    CTEParser cte_parser;
    CollationParser collation_parser;
    AggParser agg_parser;
    FuncParser func_parser;
public:
	explicit SelectParser();

    void
    markTargetListOrigin(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGVar *var, int levelsup);

    void
    markTargetListOrigins(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);

    duckdb_libpgquery::PGQuery *
    transformStmt(PGParseState *pstate, duckdb_libpgquery::PGNode *parseTree);

    duckdb_libpgquery::PGQuery *
    transformSelectStmt(PGParseState *pstate, duckdb_libpgquery::PGSelectStmt *stmt);

    duckdb_libpgquery::PGQuery *
    parse_sub_analyze(duckdb_libpgquery::PGNode *parseTree, PGParseState *parentParseState,
				  duckdb_libpgquery::PGCommonTableExpr *parentCTE,
				  duckdb_libpgquery::PGLockingClause *lockclause_from_parent,
				  bool resolve_unknowns);
};

}