#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>

namespace DB
{

class NodeParser
{
private:

public:
	explicit NodeParser();

    duckdb_libpgquery::PGConst *
    make_const(PGParseState *pstate, duckdb_libpgquery::PGValue *value, int location);

    int
    parser_errposition(PGParseState *pstate, int location);

    Oid transformArrayType(Oid *arrayType, int32 *arrayTypmod);
};

}