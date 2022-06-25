#pragma once

#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/md/CMDScCmpGPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeOidGPDB.h"
#include "naucrates/md/CMDidGPDB.h"
#include "naucrates/md/CMDName.h"

#include <Interpreters/Context.h>

#include <map>

namespace DB
{

class TypeProvider;
class gpmd::IMDType;

using IMDTypePtr = std::shared_ptr<const gpmd::IMDType>;
using TypeProviderPtr = std::shared_ptr<const TypeProvider>;


class TypeProvider
{
public:
	explicit TypeProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	static IMDTypePtr getTypeByOID(OID oid);
	static IMDTypePtr getType(Field::Types::Which which);
private:

	static int TYPE_OID_ID;
	static std::pair<OID, IMDTypePtr> TYPE_FLOAT32;
	static std::pair<OID, IMDTypePtr> TYPE_FLOAT64;
	static std::pair<OID, IMDTypePtr> TYPE_BOOLEAN;
	static std::pair<OID, IMDTypePtr> TYPE_UINT8;
	static std::pair<OID, IMDTypePtr> TYPE_UINT16;
	static std::pair<OID, IMDTypePtr> TYPE_UINT32;
	static std::pair<OID, IMDTypePtr> TYPE_UINT64;
	static std::pair<OID, IMDTypePtr> TYPE_UINT128;
	static std::pair<OID, IMDTypePtr> TYPE_UINT256;
	static std::pair<OID, IMDTypePtr> TYPE_INT8;
	static std::pair<OID, IMDTypePtr> TYPE_INT16;
	static std::pair<OID, IMDTypePtr> TYPE_INT32;
	static std::pair<OID, IMDTypePtr> TYPE_INT64;
	static std::pair<OID, IMDTypePtr> TYPE_INT128;
	static std::pair<OID, IMDTypePtr> TYPE_INT256;
	static std::pair<OID, IMDTypePtr> TYPE_STRING;
	static std::pair<OID, IMDTypePtr> TYPE_ARRAY;
	static std::pair<OID, IMDTypePtr> TYPE_TUPLE;
	static std::pair<OID, IMDTypePtr> TYPE_DECIMAL32;
	static std::pair<OID, IMDTypePtr> TYPE_DECIMAL64;
	static std::pair<OID, IMDTypePtr> TYPE_DECIMAL128;
	static std::pair<OID, IMDTypePtr> TYPE_DECIMAL256;
	static std::pair<OID, IMDTypePtr> TYPE_AGGFUNCSTATE;
	static std::pair<OID, IMDTypePtr> TYPE_MAP;
	static std::pair<OID, IMDTypePtr> TYPE_UUID;

	using Map = std::map<OID, IMDTypePtr>;

	Map oid_types_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;

	gpos::CMDName *
	CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
};

}
