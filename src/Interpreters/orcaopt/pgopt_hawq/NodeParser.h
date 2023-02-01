#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

namespace DB
{

class NodeParser
{
private:

public:
	explicit NodeParser();

    duckdb_libpgquery::PGConst * make_const(PGParseState * pstate,
		duckdb_libpgquery::PGValue * value, int location);
};

}