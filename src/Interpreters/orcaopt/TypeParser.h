#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class NodeParser;
class TypeProvider;
class RelationProvider;
class FunctionProvider;

using RelationParserPtr = std::shared_ptr<RelationParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;
using FunctionProviderPtr = std::shared_ptr<FunctionProvider>;

class TypeParser
{
private:
    RelationParserPtr relation_parser;
    NodeParserPtr node_parser;
    TypeProviderPtr type_provider;
    RelationProviderPtr relation_provider;
    FunctionProviderPtr function_provider;
public:
	explicit TypeParser();

    Oid typenameTypeId(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName);

    void
    typenameTypeIdAndMod(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p);
    
    PGTypePtr
    typenameType(PGParseState *pstate, const duckdb_libpgquery::PGTypeName *typeName, int32 *typmod_p);

    PGTypePtr LookupTypeName(PGParseState * pstate, const duckdb_libpgquery::PGTypeName * typeName,
        int32 * typmod_p, bool missing_ok);

    PGTypePtr LookupTypeNameExtended(PGParseState * pstate, const duckdb_libpgquery::PGTypeName * typeName,
        int32 * typmod_p, bool temp_ok, bool missing_ok);

    std::string
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

    Oid typeidTypeRelid(Oid type_id);

    Oid
    typeTypeId(PGTypePtr tp);

    Oid
    LookupCollation(PGParseState *pstate, duckdb_libpgquery::PGList *collnames, int location);

    std::string printTypmod(const char * typname, int32 typmod, Oid typmodout);

    std::string format_type_with_typemod(Oid type_oid, int32 typemod);

    std::string format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify);
};

}
