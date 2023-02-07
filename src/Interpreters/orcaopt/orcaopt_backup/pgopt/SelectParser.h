#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/ClauseParser.h>
#include <Interpreters/orcaopt/pgopt/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt/TargetParser.h>

namespace DB
{

class SelectParser
{
private:
    ClauseParser clause_parser;
    TargetParser target_parser;
    ClauseParser clause_parser;
    CoerceParser coerce_parser;
public:
	explicit SelectParser();

    duckdb_libpgquery::PGQuery *
    transformSelectStmt(PGParseState *pstate, duckdb_libpgquery::PGSelectStmt *stmt);

    duckdb_libpgquery::PGQuery *parse_sub_analyze(duckdb_libpgquery::PGNode *parseTree, PGParseState *parentParseState,
				  duckdb_libpgquery::PGCommonTableExpr *parentCTE,
				  duckdb_libpgquery::PGLockingClause *lockclause_from_parent);
                  
    duckdb_libpgquery::PGQuery *
    transformStmt(PGParseState *pstate, duckdb_libpgquery::PGNode *parseTree);
};

}