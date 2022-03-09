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
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDAggregate.h"

#include <Interpreters/Context.h>

#include <map>

namespace DB
{

class FunctionProvider;
class gpmd::IMDFunction;

using IMDFunctionPtr = std::shared_ptr<const gpmd::IMDFunction>;
using FunctionProviderPtr = std::shared_ptr<const FunctionProvider>;


class FunctionProvider
{
private:
	using Map = std::map<OID, IMDFunctionPtr>;

	Map oid_fun_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;

	gpos::CMDName *
	CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
public:
	explicit FunctionProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	IMDFunctionPtr getFunctionByOID(OID oid);
};

}
