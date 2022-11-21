#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>

namespace DB
{

class CollationParser
{
private:
    

public:
	explicit CollationParser();

    void
	assign_expr_collations(PGParseState *pstate, duckdb_libpgquery::PGNode *expr);
};

}