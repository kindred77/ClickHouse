#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <naucrates/md/IMDType.h>
#include <naucrates/md/CMDName.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{

class FunctionProvider;
class TypeParser;
using IMDTypePtr = std::shared_ptr<const gpmd::IMDType>;
using FunctionProviderPtr = std::unique_ptr<FunctionProvider>;
using TypeParserPtr = std::unique_ptr<TypeParser>;

class TypeProvider
{
public:
	explicit TypeProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	static IMDTypePtr getTypeByOID(Oid oid);
	PGTypePtr getTypeByOid(Oid oid) const;
	static IMDTypePtr getType(Field::Types::Which which);

    Oid getBaseType(Oid typid);

    Oid getBaseTypeAndTypmod(Oid typid, int32 * typmod);

    void get_type_category_preferred(Oid typid, char * typcategory, bool * typispreferred);

    char * format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify);

    char * format_type_be(Oid type_oid);

    char * printTypmod(const char * typname, int32 typmod, Oid typmodout);

    bool TypeIsVisible(Oid typid);

    TupleDesc lookup_rowtype_tupdesc(Oid type_id, int32 typmod);

    Oid get_element_type(Oid typid);

    void getTypeOutputInfo(Oid type, Oid * typOutput, bool * typIsVarlena);

    void getTypeInputInfo(Oid type, Oid * typInput, Oid * typIOParam);

    bool typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId);

private:
	FunctionProviderPtr function_provider;
	TypeParserPtr type_parser;
	static int TYPE_OID_ID;
	static std::pair<Oid, IMDTypePtr> TYPE_FLOAT32;
	static std::pair<Oid, IMDTypePtr> TYPE_FLOAT64;
	static std::pair<Oid, IMDTypePtr> TYPE_BOOLEAN;
	static std::pair<Oid, IMDTypePtr> TYPE_UINT8;
	static std::pair<Oid, IMDTypePtr> TYPE_UINT16;
	static std::pair<Oid, IMDTypePtr> TYPE_UINT32;
	static std::pair<Oid, IMDTypePtr> TYPE_UINT64;
	static std::pair<Oid, IMDTypePtr> TYPE_UINT128;
	static std::pair<Oid, IMDTypePtr> TYPE_UINT256;
	static std::pair<Oid, IMDTypePtr> TYPE_INT8;
	static std::pair<Oid, IMDTypePtr> TYPE_INT16;
	static std::pair<Oid, IMDTypePtr> TYPE_INT32;
	static std::pair<Oid, IMDTypePtr> TYPE_INT64;
	static std::pair<Oid, IMDTypePtr> TYPE_INT128;
	static std::pair<Oid, IMDTypePtr> TYPE_INT256;
	static std::pair<Oid, IMDTypePtr> TYPE_STRING;
	static std::pair<Oid, IMDTypePtr> TYPE_ARRAY;
	static std::pair<Oid, IMDTypePtr> TYPE_TUPLE;
	static std::pair<Oid, IMDTypePtr> TYPE_DECIMAL32;
	static std::pair<Oid, IMDTypePtr> TYPE_DECIMAL64;
	static std::pair<Oid, IMDTypePtr> TYPE_DECIMAL128;
	static std::pair<Oid, IMDTypePtr> TYPE_DECIMAL256;
	static std::pair<Oid, IMDTypePtr> TYPE_AGGFUNCSTATE;
	static std::pair<Oid, IMDTypePtr> TYPE_MAP;
	static std::pair<Oid, IMDTypePtr> TYPE_UUID;

	using Map = std::map<Oid, IMDTypePtr>;

	Map oid_types_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;

	gpmd::CMDName *
	CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
};

}
