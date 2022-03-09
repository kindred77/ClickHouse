#include <Interpreters/orcaopt/FunctionProvider.h>

#include "naucrates/dxl/CDXLUtils.h"

namespace DB
{

gpos::CMDName *
FunctionProvider::CreateMDName(const char *name_str)
{
	gpos::CWStringDynamic *str_name =
			gpdxl::CDXLUtils::CreateDynamicStringFromCharArray(mp, name_str);
	gpos::CMDName *mdname = GPOS_NEW(mp) gpos::CMDName(mp, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
}

FunctionProvider::FunctionProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
		: context(std::move(context_)),
		  mp(std::move(mp_))
{
	gpmd::IMdIdArray *arg_type_mdids1 = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids1->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
	//float32
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(1), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(1)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(3),
			/*arg_type_mdids*/ arg_type_mdids1, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	gpmd::IMdIdArray *arg_type_mdids2 = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids2->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
	//float64
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(2), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(2)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(4),
			/*arg_type_mdids*/ arg_type_mdids2, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	gpmd::IMdIdArray *arg_type_mdids3 = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids3->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
	//Int64
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(3), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(15),
			/*arg_type_mdids*/ arg_type_mdids3, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));
}

IMDFunctionPtr
FunctionProvider::getFunctionByOID(OID oid)
{
	auto it = oid_types_map.find(oid);
	if (it == oid_types_map.end())
	    return {};
	return it->second;
}

}




