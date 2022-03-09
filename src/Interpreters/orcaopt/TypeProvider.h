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
private:
	using Map = std::map<OID, IMDTypePtr>;

	Map oid_types_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;

	gpos::CMDName *
	CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
public:
	explicit TypeProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	IMDTypePtr getTypeByOID(OID oid);
};

}
