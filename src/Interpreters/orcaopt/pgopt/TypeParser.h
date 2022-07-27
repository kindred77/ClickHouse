#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>

namespace DB
{

class TypeParser
{
private:

public:
	explicit TypeParser();

    void
    typenameTypeIdAndMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p);

    int32
    typenameTypeMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, HeapTuple typ);
};

}