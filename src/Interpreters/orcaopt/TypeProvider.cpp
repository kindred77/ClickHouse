#include <Interpreters/orcaopt/TypeProvider.h>

#include "naucrates/dxl/CDXLUtils.h"

namespace DB
{

gpos::CMDName *
TypeProvider::CreateMDName(const char *name_str)
{
	gpos::CWStringDynamic *str_name =
			gpdxl::CDXLUtils::CreateDynamicStringFromCharArray(mp, name_str);
	gpos::CMDName *mdname = GPOS_NEW(mp) gpos::CMDName(mp, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
}

TypeProvider::TypeProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
		: context(std::move(context_)),
		  mp(std::move(mp_))
{
	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(3), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("Float32"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(4), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(4)), /*mdname*/ CreateMDName("Float64"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(5), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(5)), /*mdname*/ CreateMDName("Boolean"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 1,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 1)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(6), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(6)), /*mdname*/ CreateMDName("UInt8"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 1,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 1)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(7), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(7)), /*mdname*/ CreateMDName("UInt16"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 2,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 2)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(8), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(8)), /*mdname*/ CreateMDName("UInt32"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(9), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(9)), /*mdname*/ CreateMDName("UInt64"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(9), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(9)), /*mdname*/ CreateMDName("UInt64"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(10), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(10)), /*mdname*/ CreateMDName("UInt128"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 16,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 16)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(11), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(11)), /*mdname*/ CreateMDName("UInt256"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(12), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(12)), /*mdname*/ CreateMDName("Int8"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 1,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 1)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(13), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(13)), /*mdname*/ CreateMDName("Int16"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 2,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 2)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(14), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(14)), /*mdname*/ CreateMDName("Int32"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(15), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(15)), /*mdname*/ CreateMDName("Int64"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(16), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(16)), /*mdname*/ CreateMDName("Int128"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 16,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 16)));

	oid_types_map.insert(std::pair<OID, IMDTypePtr>(OID(17), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(17)), /*mdname*/ CreateMDName("Int256"), /*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, /*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, /*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, /*mdid_op_gt*/ mdid_op_gt,
			/*mdid_op_geq*/ mdid_op_geq, /*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, /*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), /*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, /*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)));
}

IMDTypePtr
TypeProvider::getTypeByOID(OID oid)
{
	auto it = oid_types_map.find(oid);
	if (it == oid_types_map.end())
	    return {};
	return it->second;
}

}




