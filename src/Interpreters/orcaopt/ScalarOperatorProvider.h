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
#include "naucrates/md/IMDScalarOp.h"

#include <Interpreters/Context.h>

#include <map>

/*
 *
select op.oid,op.oprname,op.oprkind,op.oprcanmerge,
op.oprcanhash,
(select pt.typname from pg_type pt where pt.oid=op.oprleft) as left_type,
(select pt.typname from pg_type pt where pt.oid=op.oprright) as right_type,
(select pt.typname from pg_type pt where pt.oid=op.oprresult) as result_type,
op.oprcom,op.oprnegate,
(select pr.proname from pg_proc pr where pr.oid=op.oprcode) as proc_name,
(select pr.proretset from pg_proc pr where pr.oid=op.oprcode) as is_return_set
from pg_operator op
where op.oprname in ('+','-','*','/','%','==','!=','<>','<=','>=','<','>','=')
order by 2,3,4,5,6,7,8,9,10,11,12;
 */

namespace DB
{

class ScalarOperatorProvider;
class gpmd::IMDFunction;

using IMDScalarOperatorPtr = std::shared_ptr<const gpmd::IMDScalarOp>;
using ScalarOperatorProviderPtr = std::shared_ptr<const ScalarOperatorProvider>;


class ScalarOperatorProvider
{
private:
	using Map = std::map<OID, IMDScalarOperatorPtr>;

	Map oid_scop_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;

	gpos::CMDName *
	CreateMDName(gpos::CMemoryPool *mp, const char *name_str);
public:
	explicit ScalarOperatorProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	IMDScalarOperatorPtr getScalarOperatorByOID(OID oid);
};

}
