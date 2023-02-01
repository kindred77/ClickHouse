#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

namespace DB
{

typedef HeapTuple Type;

class TypeParser
{
private:

public:
	explicit TypeParser();

    Oid typeidTypeRelid(Oid type_id);

    Oid LookupTypeName(PGParseState * pstate, const duckdb_libpgquery::PGTypeName * typname);

    Oid typenameTypeId(PGParseState * pstate, const duckdb_libpgquery::PGTypeName * typname);
};

}