#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/RelationParser.h>
#include <Interpreters/orcaopt/pgopt/NodeParser.h>

namespace DB
{

typedef HeapTuple Type;

class TypeParser
{
private:
    RelationParser relation_parser;
    NodeParser node_parser;
public:
	explicit TypeParser();

    void
    typenameTypeIdAndMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p);

    int32
    typenameTypeMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, Type typ);

    Oid
    LookupCollation(PGParseState *pstate, duckdb_libpgquery::PGList *collnames, int location);

    Oid
    typeidTypeRelid(Oid type_id);

    Type
    LookupTypeNameExtended(PGParseState *pstate,
					   const duckdb_libpgquery::PGTypeName *typeName, int32 *typmod_p,
					   bool temp_ok, bool missing_ok);
};

}