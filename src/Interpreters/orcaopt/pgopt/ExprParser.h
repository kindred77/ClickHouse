#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>

namespace DB
{

class ExprParser
{
private:

public:
	explicit ExprParser();

    duckdb_libpgquery::PGNode *
    transformExpr(PGParseState *pstate, duckdb_libpgquery::PGNode *expr, PGParseExprKind exprKind);

    duckdb_libpgquery::PGNode *
    transformExprRecurse(PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

    duckdb_libpgquery::PGNode *
    transformColumnRef(PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref);
};

}