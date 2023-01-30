#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

namespace DB
{

class ExprParser
{
private:

    bool		operator_precedence_warning = false;
    bool		Transform_null_equals = false;
public:
	explicit ExprParser();

    duckdb_libpgquery::PGNode * transformExpr(PGParseState * pstate,
        duckdb_libpgquery::PGNode * expr);
};

}