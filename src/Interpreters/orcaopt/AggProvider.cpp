#include <Interpreters/orcaopt/AggProvider.h>

#include "naucrates/dxl/CDXLUtils.h"

namespace DB
{

gpos::CMDName *
AggProvider::CreateMDName(const char *name_str)
{
	gpos::CWStringDynamic *str_name =
			gpdxl::CDXLUtils::CreateDynamicStringFromCharArray(mp, name_str);
	gpos::CMDName *mdname = GPOS_NEW(mp) gpos::CMDName(mp, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
}

AggProvider::AggProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
		: context(std::move(context_)),
		  mp(std::move(mp_))
{
	//sum float32
	oid_agg_map.insert(std::pair<OID, IMDAggregatePtr>(OID(1), GPOS_NEW(mp) gpos::CMDAggregateGPDB(
			mp, gpos::CMDIdGPDB(OID(1)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(3),
			/*intermediate_result_type_mdid*/ OID(3), /*is_ordered*/ false, /*is_splittable*/ is_splittable,
			/*is_hash_agg_capable*/ is_hash_agg_capable)));

	//sum float32
	oid_agg_map.insert(std::pair<OID, IMDAggregatePtr>(OID(2), GPOS_NEW(mp) gpos::CMDAggregateGPDB(
			mp, gpos::CMDIdGPDB(OID(2)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(4),
			/*intermediate_result_type_mdid*/ OID(4), /*is_ordered*/ false, /*is_splittable*/ is_splittable,
			/*is_hash_agg_capable*/ is_hash_agg_capable)));

	//sum Int64
	oid_agg_map.insert(std::pair<OID, IMDAggregatePtr>(OID(3), GPOS_NEW(mp) gpos::CMDAggregateGPDB(
			mp, gpos::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(15),
			/*intermediate_result_type_mdid*/ OID(15), /*is_ordered*/ false, /*is_splittable*/ true,
			/*is_hash_agg_capable*/ true)));
}

IMDAggregatePtr
AggProvider::getTypeByOID(OID oid)
{
	auto it = oid_types_map.find(oid);
	if (it == oid_types_map.end())
	    return {};
	return it->second;
}

}




