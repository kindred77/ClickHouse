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
	/*-------------plus start---------------------------*/
	//float32+float32=float32
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(3)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(1), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(1)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(3)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Float64+Float64=Float64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(4)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(2), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(2)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(4)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Boolean+Boolean=UInt64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(5)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(3), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//UInt8+UInt8=UInt64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(6)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(4), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(4)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//UInt16+UInt16=UInt64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(7)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(5), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(5)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//UInt32+UInt32=UInt64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(8)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(6), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(6)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//UInt64+UInt64=UInt64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(9)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(7), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(7)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//UInt128+UInt128=UInt128
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(10)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(8), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(8)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(10)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//UInt256+UInt256=UInt256
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(11)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(9), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(9)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(11)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Int8+Int8=Int64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(12)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(10), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(10)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Int16+Int16=Int64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(13)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(11), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(11)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Int32+Int32=Int64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(14)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(12), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(12)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Int64+Int64=Int64
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(15)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(13), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(13)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Int128+Int128=Int128
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(16)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(14), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(14)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(16)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Int256+Int256=Int256
	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(17)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(15), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(15)), /*mdname*/ CreateMDName("plus"),
			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(17)),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	/*-------------plus end---------------------------*/

	//Float64
	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(4)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(2), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(2)), /*mdname*/ CreateMDName("minus"),
			/*result_type_mdid*/ OID(4),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable,
			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	//Boolean
	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(5)));
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(3), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("multiply"), /*result_type_mdid*/ OID(3),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
	//float32
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(4), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(4)), /*mdname*/ CreateMDName("divide"), /*result_type_mdid*/ OID(3),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
	//float64
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(5), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(5)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(4),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
	//Int64
	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(6), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
			mp, gpos::CMDIdGPDB(OID(6)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(15),
			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
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




