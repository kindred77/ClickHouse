#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>

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
    
    Type
    typenameType(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, int32 *typmod_p);

    Type
    LookupTypeName(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName,
			   int32 *typmod_p, bool missing_ok);

    char *
    TypeNameToString(const duckdb_libpgquery::PGTypeName *typeName);

    int32
    typenameTypeMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, Type typ);

    Type
    typeidType(Oid id);

    Oid
    typeTypeCollation(Type typ);

    int16
    typeLen(Type t);

    bool
    typeByVal(Type t);

    Datum
    stringTypeDatum(Type tp, char *string, int32 atttypmod);

    Oid
    typeOrDomainTypeRelid(Oid type_id);

    Oid
    typeTypeRelid(Type typ);

    Oid
    typeTypeId(Type tp);

    Oid
    LookupCollation(PGParseState *pstate, duckdb_libpgquery::PGList *collnames, int location);
};

}