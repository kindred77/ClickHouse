#include <Interpreters/orcaopt/ScalarOperatorProvider.h>

#include "naucrates/dxl/CDXLUtils.h"

namespace DB
{

gpos::CMDName *
ScalarOperatorProvider::CreateMDName(const char *name_str)
{
	gpos::CWStringDynamic *str_name =
			gpdxl::CDXLUtils::CreateDynamicStringFromCharArray(mp, name_str);
	gpos::CMDName *mdname = GPOS_NEW(mp) gpos::CMDName(mp, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
}

ScalarOperatorProvider::ScalarOperatorProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
		: context(std::move(context_)),
		  mp(std::move(mp_))
{
	oid_scop_map.insert(
		std::pair<OID, IMDFunctionPtr>(OID(1),GPOS_NEW(m_mp) CDXLScalarComp(
			m_mp, gpos::CMDIdGPDB(OID(1)), GPOS_NEW(m_mp) CWStringConst("="))
		)
	);
	oid_scop_map.insert(
		std::pair<OID, IMDFunctionPtr>(OID(2),GPOS_NEW(m_mp) CDXLScalarComp(
			m_mp, gpos::CMDIdGPDB(OID(2)), GPOS_NEW(m_mp) CWStringConst("!="))
		)
	);

	/*------------------ + start ------------------------*/
	//float32+float32=float32
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(3)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(3)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(3)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(1)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);
	
	//float64+float64=float64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(4)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(4)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(4)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(4)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(2)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//Boolean+Boolean=UInt64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(5)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(5)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(5)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(9)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(3)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//UInt8+UInt8=UInt64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(6)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(6)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(6)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(9)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(4)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//UInt16+UInt16=UInt64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(7)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(7)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(7)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(9)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(5)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//UInt32+UInt32=UInt64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(8)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(8)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(8)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(9)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(6)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//UInt64+UInt64=UInt64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(7)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(9)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(9)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(9)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(7)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//UInt128+UInt128=UInt128
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(8)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(10)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(10)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(10)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(8)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//UInt256+UInt256=UInt256
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(9)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(11)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(11)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(11)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(9)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//Int8+Int8=Int64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(10)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(12)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(12)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(15)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(10)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//Int16+Int16=Int64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(11)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(13)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(13)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(15)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(11)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//Int32+Int32=Int64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(12)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(14)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(14)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(15)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(12)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//Int64+Int64=Int64
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(13)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(15)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(15)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(15)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(13)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//Int128+Int128=Int128
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(14)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(16)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(16)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(16)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(14)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);

	//Int256+Int256=Int256
	gpmd::CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) gpmd::CMDScalarOpGPDB(
		mp, gpos::CMDIdGPDB(OID(15)), /*mdname*/ CreateMDName("+"),
		/*mdid_type_left*/ gpmd::CMDIdGPDB(OID(17)),
		/*mdid_type_right*/ gpmd::CMDIdGPDB(OID(17)),
		/*result_type_mdid*/ gpmd::CMDIdGPDB(OID(17)),
		/*mdid_func*/ gpos::CMDIdGPDB(OID(15)),
		mdid_commute_opr, m_mdid_inverse_opr, IMDType::EcmptOther,
		/*returns_null_on_null_input*/ true, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);
	/*------------------ + end ------------------------*/
};

CMDScalarOpGPDBPtr
ScalarOperatorProvider::getScalarOperatorByOID(OID oid)
{
	auto it = oid_types_map.find(oid);
	if (it == oid_types_map.end())
	    return {};
	return it->second;
};

std::pair<OID, CMDScalarOpGPDBPtr>
ScalarOperatorProvider::getScalarOperatorByNameAndArgTypes(
	const char * op_name,
	OID left_type,
	OID right_type
)
{
	for (Map::iterator it = oid_scop_map.begin(); ++it; it != oid_scop_map.end())
	{

	}
};

OID
ScalarOperatorProvider::get_commutator(OID operator_oid)
{
	auto it = oid_types_map.find(oid);
	if (it == oid_types_map.end())
	    return InvalidOid;
	return it->second->GetCommuteOpMdid().Oid();
};

}




