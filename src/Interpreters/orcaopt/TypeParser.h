#pragma once

#include <common/parser_common.hpp>

namespace DB
{

// class RelationParser;
// class NodeParser;
// class TypeProvider;
// class RelationProvider;
// class FunctionProvider;

// using RelationParserPtr = std::shared_ptr<RelationParser>;
// using NodeParserPtr = std::shared_ptr<NodeParser>;

// class Context;
// using ContextPtr = std::shared_ptr<const Context>;

class TypeParser
{
private:
    // RelationParserPtr relation_parser;
    // NodeParserPtr node_parser;

    // ContextPtr context;
public:
	//explicit TypeParser(const ContextPtr& context_);

    static duckdb_libpgquery::PGOid typenameTypeId(duckdb_libpgquery::PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName);

    static void
    typenameTypeIdAndMod(duckdb_libpgquery::PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName,
					 duckdb_libpgquery::PGOid *typeid_p, duckdb_libpgquery::int32 *typmod_p);
    
    static duckdb_libpgquery::PGTypePtr
    typenameType(duckdb_libpgquery::PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, duckdb_libpgquery::int32 *typmod_p);

    static duckdb_libpgquery::PGTypePtr LookupTypeName(duckdb_libpgquery::PGParseState * pstate, const duckdb_libpgquery::PGTypeName * typeName,
        duckdb_libpgquery::int32 * typmod_p, bool missing_ok);

    static duckdb_libpgquery::PGTypePtr LookupTypeNameExtended(duckdb_libpgquery::PGParseState * pstate, const duckdb_libpgquery::PGTypeName * typeName,
        duckdb_libpgquery::int32 * typmod_p, bool temp_ok, bool missing_ok);

    static std::string
    TypeNameToString(const duckdb_libpgquery::PGTypeName *typeName);

    static duckdb_libpgquery::int32
    typenameTypeMod(duckdb_libpgquery::PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, duckdb_libpgquery::PGTypePtr typ);

    static duckdb_libpgquery::PGTypePtr
    typeidType(duckdb_libpgquery::PGOid id);

    static duckdb_libpgquery::PGOid
    typeTypeCollation(duckdb_libpgquery::PGTypePtr typ);

    static duckdb_libpgquery::int16
    typeLen(duckdb_libpgquery::PGTypePtr t);

    static bool
    typeByVal(duckdb_libpgquery::PGTypePtr t);

    static duckdb_libpgquery::Datum
    stringTypeDatum(duckdb_libpgquery::PGTypePtr tp, const char *str, duckdb_libpgquery::int32 atttypmod);

    static duckdb_libpgquery::PGOid
    typeOrDomainTypeRelid(duckdb_libpgquery::PGOid type_id);

    static duckdb_libpgquery::PGOid
    typeTypeRelid(duckdb_libpgquery::PGTypePtr typ);

    static duckdb_libpgquery::PGOid typeidTypeRelid(duckdb_libpgquery::PGOid type_id);

    static duckdb_libpgquery::PGOid
    typeTypeId(duckdb_libpgquery::PGTypePtr tp);

    static duckdb_libpgquery::PGOid
    LookupCollation(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *collnames, int location);
};

}
