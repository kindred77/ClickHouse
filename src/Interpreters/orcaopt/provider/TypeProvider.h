#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <naucrates/md/IMDType.h>
#include <naucrates/md/CMDName.h>

#include <gpos/memory/CMemoryPool.h>

#include <algorithm>
#include <map>
#include <unordered_map>

namespace DB
{

class TypeParser;
class FunctionProvider;
class RelationProvider;
class OperProvider;
class ProcProvider;

using IMDTypePtr = std::shared_ptr<const gpmd::IMDType>;
using TypeParserPtr = std::shared_ptr<TypeParser>;
using FunctionProviderPtr = std::shared_ptr<FunctionProvider>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;
using OperProviderPtr = std::shared_ptr<OperProvider>;
using ProcProviderPtr = std::shared_ptr<ProcProvider>;

struct PGTupleDescHash
{
    std::size_t operator()(const PGTupleDescPtr & key) const;
};

struct PGTupleDescEqual
{
    bool operator()(const PGTupleDescPtr & lhs, const PGTupleDescPtr & rhs) const;
};


class TypeProvider
{
public:
	//explicit TypeProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit TypeProvider();
	//static void Init();
	//IMDTypePtr getTypeByOID(Oid oid);
	PGTypePtr getTypeByOid(Oid oid) const;
	//IMDTypePtr getType(Field::Types::Which which);

    Oid getBaseType(Oid typid);

    Oid getBaseTypeAndTypmod(Oid typid, int32 * typmod);

    void get_type_category_preferred(Oid typid, char * typcategory, bool * typispreferred);

	std::string format_type_with_typemod(Oid type_oid, int32 typemod);

    std::string format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify);

    std::string format_type_be(Oid type_oid);

	std::string printTypmod(const char * typname, int32 typmod, Oid typmodout);

    bool TypeIsVisible(Oid typid);

	PGTupleDescPtr get_tupdesc_by_type_relid(PGTypePtr type);

    PGTupleDescPtr lookup_rowtype_tupdesc_internal(Oid type_id, int32 typmod, bool noError);

    PGTupleDescPtr lookup_rowtype_tupdesc_copy(Oid type_id, int32 typmod);

    PGTupleDescPtr lookup_rowtype_tupdesc(Oid type_id, int32 typmod);

    Oid get_element_type(Oid typid);

	Oid get_typeoid_by_typename_namespaceoid(const char * type_name, Oid namespace_oid = Oid(0));

	PGTypePtr get_type_by_typename_namespaceoid(const std::string& type_name, Oid namespace_oid = Oid(0));

    void getTypeOutputInfo(Oid type, Oid * typOutput, bool * typIsVarlena);

    void getTypeInputInfo(Oid type, Oid * typInput, Oid * typIOParam);

    bool typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId);

    Oid get_range_subtype(Oid rangeOid);

    char get_typtype(Oid typid);

	bool type_is_enum(Oid typid);

    Oid get_base_element_type(Oid typid);

    Oid get_array_type(Oid typid);

    bool type_is_range(Oid typid);

    bool type_is_rowtype(Oid typid);

    Oid get_typcollation(Oid typid);

    bool type_is_collatable(Oid typid);

    void PGTupleDescInitEntry(
        PGTupleDescPtr desc, PGAttrNumber attributeNumber, const std::string & attributeName,
		Oid oidtypeid, int32 typmod, int attdim);

    TypeFuncClass get_expr_result_type(duckdb_libpgquery::PGNode * expr, Oid * resultTypeId, PGTupleDescPtr & resultTupleDesc);

    Oid TypenameGetTypidExtended(const char * typname, bool temp_ok);

    Oid getTypeIOParam(PGTypePtr typeTuple);

	TypeFuncClass
    internal_get_result_type(Oid funcid, duckdb_libpgquery::PGNode * call_expr,
        Oid * resultTypeId, PGTupleDescPtr & resultTupleDesc);

    TypeFuncClass get_type_func_class(Oid typid);

    PGTupleDescPtr build_function_result_tupdesc_t(PGProcPtr & procTuple);

    PGTupleDescPtr build_function_result_tupdesc_d(PGProcPtr & procTuple);

    Oid get_call_expr_argtype(duckdb_libpgquery::PGNode * expr, int argnum);

    Oid resolve_generic_type(Oid declared_type, Oid context_actual_type, Oid context_declared_type);

    void assign_record_type_typmod(PGTupleDescPtr tupDesc);

    bool resolve_polymorphic_tupdesc(PGTupleDescPtr tupdesc, std::vector<Oid> & declared_args, duckdb_libpgquery::PGNode * call_expr);

private:
	FunctionProviderPtr function_provider;
	TypeParserPtr type_parser;
	RelationProviderPtr relation_provider;
	OperProviderPtr oper_provider;
	ProcProviderPtr proc_provider;

	std::unordered_map<PGTupleDescPtr, const PGTupleDesc &, PGTupleDescHash, PGTupleDescEqual> recordCacheHash;
	size_t NextRecordTypmod = 0;
	std::vector<PGTupleDescPtr> recordCacheArray;

	static int TYPE_OID_ID;
	static std::pair<Oid, PGTypePtr> TYPE_FLOAT32;
	static std::pair<Oid, PGTypePtr> TYPE_FLOAT64;
	static std::pair<Oid, PGTypePtr> TYPE_BOOLEAN;
	// static std::pair<Oid, PGTypePtr> TYPE_UINT8;
	// static std::pair<Oid, PGTypePtr> TYPE_UINT16;
	// static std::pair<Oid, PGTypePtr> TYPE_UINT32;
	// static std::pair<Oid, PGTypePtr> TYPE_UINT64;
	// static std::pair<Oid, PGTypePtr> TYPE_UINT128;
	// static std::pair<Oid, PGTypePtr> TYPE_UINT256;
	// static std::pair<Oid, PGTypePtr> TYPE_INT8;
	static std::pair<Oid, PGTypePtr> TYPE_INT16;
	static std::pair<Oid, PGTypePtr> TYPE_INT32;
	static std::pair<Oid, PGTypePtr> TYPE_INT64;
	// static std::pair<Oid, PGTypePtr> TYPE_INT128;
	// static std::pair<Oid, PGTypePtr> TYPE_INT256;
	static std::pair<Oid, PGTypePtr> TYPE_STRING;
	static std::pair<Oid, PGTypePtr> TYPE_FIXEDSTRING;
	static std::pair<Oid, PGTypePtr> TYPE_DATE;
	static std::pair<Oid, PGTypePtr> TYPE_DATETIME;
	static std::pair<Oid, PGTypePtr> TYPE_DATETIME64;
	// static std::pair<Oid, PGTypePtr> TYPE_ARRAY;
	// static std::pair<Oid, PGTypePtr> TYPE_TUPLE;
	// static std::pair<Oid, PGTypePtr> TYPE_DECIMAL32;
	static std::pair<Oid, PGTypePtr> TYPE_DECIMAL64;
	// static std::pair<Oid, PGTypePtr> TYPE_DECIMAL128;
	// static std::pair<Oid, PGTypePtr> TYPE_DECIMAL256;
	// static std::pair<Oid, PGTypePtr> TYPE_AGGFUNCSTATE;
	// static std::pair<Oid, PGTypePtr> TYPE_MAP;
	// static std::pair<Oid, PGTypePtr> TYPE_UUID;

	using Map = std::map<Oid, PGTypePtr>;

	Map oid_type_map;
	//ContextPtr context;
	//gpos::CMemoryPool *mp;

	gpmd::CMDName *
	CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
};

}
