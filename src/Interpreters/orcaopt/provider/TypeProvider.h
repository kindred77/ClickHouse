#pragma once

#include <common/parser_common.hpp>
#include <Interpreters/Context.h>

#include <naucrates/md/IMDType.h>
#include <naucrates/md/CMDName.h>

#include <gpos/memory/CMemoryPool.h>

#include <algorithm>
#include <map>
#include <unordered_map>

namespace DB
{

//class TypeParser;
//class FunctionProvider;
class RelationProvider;
class OperProvider;
class ProcProvider;

using IMDTypePtr = std::shared_ptr<const gpmd::IMDType>;
//using TypeParserPtr = std::shared_ptr<TypeParser>;
//using FunctionProviderPtr = std::shared_ptr<FunctionProvider>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;
using OperProviderPtr = std::shared_ptr<OperProvider>;
using ProcProviderPtr = std::shared_ptr<ProcProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

struct PGTupleDescHash
{
    std::size_t operator()(const duckdb_libpgquery::PGTupleDescPtr & key) const;
};

struct PGTupleDescEqual
{
    bool operator()(const duckdb_libpgquery::PGTupleDescPtr & lhs, const duckdb_libpgquery::PGTupleDescPtr & rhs) const;
};


class TypeProvider
{
public:
	//explicit TypeProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	//explicit TypeProvider(const ContextPtr& context_);
	//static void Init();
	//IMDTypePtr getTypeByOID(PGOid oid);
	static duckdb_libpgquery::PGTypePtr getTypeByOid(duckdb_libpgquery::PGOid oid);
	static bool TypeExists(duckdb_libpgquery::PGOid oid);
	static std::optional<std::string> get_type_name(duckdb_libpgquery::PGOid oid);
	//IMDTypePtr getType(Field::Types::Which which);

    static duckdb_libpgquery::PGOid getBaseType(duckdb_libpgquery::PGOid typid);

    static duckdb_libpgquery::PGOid getBaseTypeAndTypmod(duckdb_libpgquery::PGOid typid, duckdb_libpgquery::int32 * typmod);

    static void get_type_category_preferred(duckdb_libpgquery::PGOid typid, char * typcategory, bool * typispreferred);

	static std::string format_type_with_typemod(duckdb_libpgquery::PGOid type_oid, duckdb_libpgquery::int32 typemod);

    static std::string format_type_internal(duckdb_libpgquery::PGOid type_oid, duckdb_libpgquery::int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify);

    static std::string format_type_be(duckdb_libpgquery::PGOid type_oid);

	static std::string printTypmod(const char * typname, duckdb_libpgquery::int32 typmod, duckdb_libpgquery::PGOid typmodout);

    static bool TypeIsVisible(duckdb_libpgquery::PGOid typid);

	static duckdb_libpgquery::PGTupleDescPtr get_tupdesc_by_type_relid(duckdb_libpgquery::PGTypePtr type);

    static duckdb_libpgquery::PGTupleDescPtr lookup_rowtype_tupdesc_internal(duckdb_libpgquery::PGOid type_id, duckdb_libpgquery::int32 typmod, bool noError);

    static duckdb_libpgquery::PGTupleDescPtr lookup_rowtype_tupdesc_copy(duckdb_libpgquery::PGOid type_id, duckdb_libpgquery::int32 typmod);

    static duckdb_libpgquery::PGTupleDescPtr lookup_rowtype_tupdesc(duckdb_libpgquery::PGOid type_id, duckdb_libpgquery::int32 typmod);

    static duckdb_libpgquery::PGOid get_element_type(duckdb_libpgquery::PGOid typid);

	static duckdb_libpgquery::PGOid get_typeoid_by_typename_namespaceoid(const char * type_name, duckdb_libpgquery::PGOid namespace_oid = duckdb_libpgquery::PGOid(0));

	static duckdb_libpgquery::PGTypePtr get_type_by_typename_namespaceoid(const std::string& type_name, duckdb_libpgquery::PGOid namespace_oid = duckdb_libpgquery::PGOid(0));

    static void getTypeOutputInfo(duckdb_libpgquery::PGOid type, duckdb_libpgquery::PGOid * typOutput, bool * typIsVarlena);

    static void getTypeInputInfo(duckdb_libpgquery::PGOid type, duckdb_libpgquery::PGOid * typInput, duckdb_libpgquery::PGOid * typIOParam);

    static bool typeInheritsFrom(duckdb_libpgquery::PGOid subclassTypeId, duckdb_libpgquery::PGOid superclassTypeId);

    static duckdb_libpgquery::PGOid get_range_subtype(duckdb_libpgquery::PGOid rangeOid);

    static char get_typtype(duckdb_libpgquery::PGOid typid);

	static bool type_is_enum(duckdb_libpgquery::PGOid typid);

    static duckdb_libpgquery::PGOid get_base_element_type(duckdb_libpgquery::PGOid typid);

    static duckdb_libpgquery::PGOid get_array_type(duckdb_libpgquery::PGOid typid);

    static bool type_is_range(duckdb_libpgquery::PGOid typid);

    static bool type_is_rowtype(duckdb_libpgquery::PGOid typid);

    static duckdb_libpgquery::PGOid get_typcollation(duckdb_libpgquery::PGOid typid);

    static bool type_is_collatable(duckdb_libpgquery::PGOid typid);

    static void PGTupleDescInitEntry(
        duckdb_libpgquery::PGTupleDescPtr desc, duckdb_libpgquery::PGAttrNumber attributeNumber, const std::string & attributeName,
		duckdb_libpgquery::PGOid oidtypeid, duckdb_libpgquery::int32 typmod, int attdim);

    static duckdb_libpgquery::TypeFuncClass get_expr_result_type(duckdb_libpgquery::PGNode * expr, duckdb_libpgquery::PGOid * resultTypeId, duckdb_libpgquery::PGTupleDescPtr & resultTupleDesc);

    static duckdb_libpgquery::PGOid TypenameGetTypidExtended(const char * typname, bool temp_ok);

    static duckdb_libpgquery::PGOid getTypeIOParam(duckdb_libpgquery::PGTypePtr typeTuple);

	static duckdb_libpgquery::TypeFuncClass
    internal_get_result_type(duckdb_libpgquery::PGOid funcid, duckdb_libpgquery::PGNode * call_expr,
        duckdb_libpgquery::PGOid * resultTypeId, duckdb_libpgquery::PGTupleDescPtr & resultTupleDesc);

    static duckdb_libpgquery::TypeFuncClass get_type_func_class(duckdb_libpgquery::PGOid typid);

    static duckdb_libpgquery::PGTupleDescPtr build_function_result_tupdesc_t(duckdb_libpgquery::PGProcPtr & procTuple);

    static duckdb_libpgquery::PGTupleDescPtr build_function_result_tupdesc_d(duckdb_libpgquery::PGProcPtr & procTuple);

    static duckdb_libpgquery::PGOid get_call_expr_argtype(duckdb_libpgquery::PGNode * expr, int argnum);

    static duckdb_libpgquery::PGOid resolve_generic_type(duckdb_libpgquery::PGOid declared_type, duckdb_libpgquery::PGOid context_actual_type, duckdb_libpgquery::PGOid context_declared_type);

    static void assign_record_type_typmod(duckdb_libpgquery::PGTupleDescPtr tupDesc);

    static bool resolve_polymorphic_tupdesc(duckdb_libpgquery::PGTupleDescPtr tupdesc, std::vector<duckdb_libpgquery::PGOid> & declared_args, duckdb_libpgquery::PGNode * call_expr);

	static void get_typlenbyval(duckdb_libpgquery::PGOid typid, duckdb_libpgquery::int16 *typlen, bool *typbyval);

private:
	//FunctionProviderPtr function_provider;
	//TypeParserPtr type_parser;
	//RelationProviderPtr relation_provider;
	//OperProviderPtr oper_provider;
	//ProcProviderPtr proc_provider;

	static std::unordered_map<duckdb_libpgquery::PGTupleDescPtr, const duckdb_libpgquery::PGTupleDesc &, PGTupleDescHash, PGTupleDescEqual> recordCacheHash;
	static size_t NextRecordTypmod;
	static std::vector<duckdb_libpgquery::PGTupleDescPtr> recordCacheArray;

	static int TYPE_OID_ID;
	
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_TIMESTAMPTZ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_NUMERIC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_TIMESTAMPTZ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_TIMESTAMP;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_DATE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_DATE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_BPCHAR;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_NUMERIC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_BPCHAR;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERNAL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_TEXT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_TIMESTAMP;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_ANY;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_BOOL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_BYTEA;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_BOOL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_CSTRING;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_OID;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_TEXT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_ANYARRAY;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_CSTRING;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_BYTEA;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_OID;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr> TYPE_INTERN_INT8;

	using OidTypeMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGTypePtr>;
	static OidTypeMap oid_type_map;

	//ContextPtr context;
	//gpos::CMemoryPool *mp;

	// static gpmd::CMDName *
	// CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
};

}
