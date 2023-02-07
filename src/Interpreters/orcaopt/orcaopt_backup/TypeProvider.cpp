#include <Interpreters/orcaopt/TypeProvider.h>

#include "naucrates/dxl/CDXLUtils.h"

namespace DB
{

	TypeProvider::TYPE_OID_ID = 3;
	std::pair<OID, IMDTypePtr> TypeProvider::TYPE_FLOAT32 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Float32)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)
		};
	std::pair<OID, IMDTypePtr> TYPE_FLOAT64 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Float64)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)
		};
	std::pair<OID, IMDTypePtr> TYPE_BOOLEAN = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int8)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 1,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 1)
		};
	std::pair<OID, IMDTypePtr> TYPE_UINT8 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::UInt8)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 1,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 1)
		};
	std::pair<OID, IMDTypePtr> TYPE_UINT16 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::UInt16)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 2,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 2)
		};
	std::pair<OID, IMDTypePtr> TYPE_UINT32 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::UInt32)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)
		};
	std::pair<OID, IMDTypePtr> TYPE_UINT64 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::UInt64)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)
		};
	std::pair<OID, IMDTypePtr> TYPE_UINT128 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::UInt128)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 16,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 16)
		};
	std::pair<OID, IMDTypePtr> TYPE_UINT256 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::UInt256)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_INT8 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int8)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 1,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 1)
		};
	std::pair<OID, IMDTypePtr> TYPE_INT16 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int16)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 2,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 2)
		};
	std::pair<OID, IMDTypePtr> TYPE_INT32 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int32)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)
		};
	std::pair<OID, IMDTypePtr> TYPE_INT64 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int64)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)
		};
	std::pair<OID, IMDTypePtr> TYPE_INT128 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int128)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 16,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 16)
		};
	std::pair<OID, IMDTypePtr> TYPE_INT256 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int256)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_STRING = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::String)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ true, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_FIXEDSTRING = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::FixedString)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ true, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_DATE = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Date)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ true, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)
		};
	std::pair<OID, IMDTypePtr> TYPE_DATETIME = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::DateTime)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ true, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)
		};
	std::pair<OID, IMDTypePtr> TYPE_DATETIME64 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::DateTime64)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ true, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)
		};
	std::pair<OID, IMDTypePtr> TYPE_ARRAY = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Array)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_TUPLE = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Tuple)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ false, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ true, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_DECIMAL32 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Decimal32)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 4,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 4)
		};
	std::pair<OID, IMDTypePtr> TYPE_DECIMAL64 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Decimal64)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 8,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 8)
		};
	std::pair<OID, IMDTypePtr> TYPE_DECIMAL128 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Decimal128)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 16,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 16)
		};
	std::pair<OID, IMDTypePtr> TYPE_DECIMAL256 = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Decimal256)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_AGGFUNCSTATE = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::AggregateFunction)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_MAP = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Map)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};
	std::pair<OID, IMDTypePtr> TYPE_UUID = {OID(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(OID(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::UUID)), 
			/*is_redistributable*/ true, /*is_fixed_length*/ true, /*length*/ 32,
			/*is_passed_by_value*/ true, /*mdid_distr_opfamily*/ mdid_distr_opfamily, 
			/*mdid_legacy_distr_opfamily*/ mdid_legacy_distr_opfamily,
			/*mdid_op_eq*/ mdid_op_eq, /*mdid_op_neq*/ mdid_op_neq, 
			/*mdid_op_lt*/ mdid_op_lt, /*mdid_op_leq*/ mdid_op_leq, 
			/*mdid_op_gt*/ mdid_op_gt, /*mdid_op_geq*/ mdid_op_geq, 
			/*mdid_op_cmp*/ mdid_op_cmp, /*mdid_min*/ mdid_min, /*mdid_max*/ mdid_max, 
			/*mdid_avg*/ mdid_avg, /*mdid_sum*/ mdid_sum,
			/*mdid_count*/ GPOS_NEW(mp) gpmd::CMDIdGPDB(COUNT_ANY_OID), 
			/*is_hashable*/ true, /*is_merge_joinable*/ true, /*is_composite_type*/ false,
			/*is_text_related_type*/ false, /*mdid_type_relid*/ 0, 
			/*mdid_type_array*/ mdid_type_array, /*gpdb_length*/ 32)
		};

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
	oid_types_map.insert(TYPE_FLOAT32);
	oid_types_map.insert(TYPE_FLOAT64);
	oid_types_map.insert(TYPE_BOOLEAN);
	oid_types_map.insert(TYPE_UINT8);
	oid_types_map.insert(TYPE_UINT16);
	oid_types_map.insert(TYPE_UINT32);
	oid_types_map.insert(TYPE_UINT64);
	oid_types_map.insert(TYPE_UINT128);
	oid_types_map.insert(TYPE_UINT256);
	oid_types_map.insert(TYPE_INT8);
	oid_types_map.insert(TYPE_INT16);
	oid_types_map.insert(TYPE_INT32);
	oid_types_map.insert(TYPE_INT64);
	oid_types_map.insert(TYPE_INT128);
	oid_types_map.insert(TYPE_INT256);
	oid_types_map.insert(TYPE_STRING);
	oid_types_map.insert(TYPE_FIXEDSTRING);
	oid_types_map.insert(TYPE_DATE);
	oid_types_map.insert(TYPE_DATETIME);
	oid_types_map.insert(TYPE_DATETIME64);
	oid_types_map.insert(TYPE_ARRAY);
	oid_types_map.insert(TYPE_TUPLE);
	oid_types_map.insert(TYPE_DECIMAL32);
	oid_types_map.insert(TYPE_DECIMAL64);
	oid_types_map.insert(TYPE_DECIMAL128);
	oid_types_map.insert(TYPE_DECIMAL256);
	oid_types_map.insert(TYPE_AGGFUNCSTATE);
	oid_types_map.insert(TYPE_MAP);
	oid_types_map.insert(TYPE_UUID);
}

IMDTypePtr
TypeProvider::getTypeByOID(OID oid)
{
	auto it = oid_types_map.find(oid);
	if (it == oid_types_map.end())
	    return {};
	return it->second;
}

IMDTypePtr
TypeProvider::getType(Field::Types::Which which)
{
	switch (which)
	{
	case Null:
		return 
    case UInt64:
		return TYPE_UINT64.second;
    case Int64:
		return TYPE_INT64.second;
    case Float64:
		return TYPE_FLOAT64.second;
    case UInt128:
		return TYPE_UINT128.second;
    case Int128:
		return TYPE_INT128.second;
    case String:
		return TYPE_STRING.second;
	case FixedString:
		return TYPE_FIXEDSTRING.second;
	case Date:
		return TYPE_DATE.second;
	case DateTime:
		return TYPE_DATETIME.second;
	case DateTime64:
		return TYPE_DATETIME64.second;
    case Array:
		return TYPE_ARRAY.second;
    case Tuple:
		return TYPE_TUPLE.second;
    case Decimal32:
		return TYPE_DECIMAL32.second;
    case Decimal64:
		return TYPE_DECIMAL64.second;
    case Decimal128:
		return TYPE_DECIMAL128.second;
    //case AggregateFunctionState:
		//return TYPE_AGGFUNCSTATE.second;
    case Decimal256:
		return TYPE_DECIMAL256.second;
    case UInt256:
		return TYPE_UINT256.second;
    case Int256:
		return TYPE_INT256.second;
    case Map:
		return TYPE_MAP.second;
    case UUID:
		return TYPE_UUID.second;
	default:
		throw Exception("Unsupported type yet.", ErrorCodes::SYNTAX_ERROR);
	}
}

}




