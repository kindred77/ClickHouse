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
#include "naucrates/md/IMDAggregate.h"

#include <Interpreters/Context.h>

#include <map>

namespace DB
{

class AggProvider;
class gpmd::IMDAggregate;

using IMDAggregatePtr = std::shared_ptr<const gpmd::IMDAggregate>;
using AggProviderPtr = std::shared_ptr<const AggProvider>;


class AggProvider
{
private:
	using Map = std::map<OID, IMDAggregatePtr>;

	Map oid_agg_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;

	gpos::CMDName *
	CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
public:
	explicit AggProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	IMDAggregatePtr getTypeByOID(OID oid);
};

}
