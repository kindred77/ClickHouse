#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class NodeParser;

using RelationParserPtr = std::unique_ptr<RelationParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;

class TypeParser
{
private:
    RelationParserPtr relation_parser;
    NodeParserPtr node_parser;
public:
	explicit TypeParser();

    void
    typenameTypeIdAndMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p);
    
    PGTypePtr
    typenameType(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, int32 *typmod_p);

    PGTypePtr
    LookupTypeName(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName,
			   int32 *typmod_p, bool missing_ok);

    char *
    TypeNameToString(const duckdb_libpgquery::PGTypeName *typeName);

    int32
    typenameTypeMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, PGTypePtr typ);

    PGTypePtr
    typeidType(Oid id);

    Oid
    typeTypeCollation(PGTypePtr typ);

    int16
    typeLen(PGTypePtr t);

    bool
    typeByVal(PGTypePtr t);

    Datum
    stringTypeDatum(PGTypePtr tp, const char *string, int32 atttypmod);

    Oid
    typeOrDomainTypeRelid(Oid type_id);

    Oid
    typeTypeRelid(PGTypePtr typ);

    Oid
    typeTypeId(PGTypePtr tp);

    Oid
    LookupCollation(PGParseState *pstate, duckdb_libpgquery::PGList *collnames, int location);

    char * format_type_with_typemod(Oid type_oid, int32 typemod);

    char * format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify);
};

}
