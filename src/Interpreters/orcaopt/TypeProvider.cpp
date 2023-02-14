#include <Interpreters/orcaopt/TypeProvider.h>
#include <Interpreters/orcaopt/FunctionProvider.h>
#include <Interpreters/orcaopt/TypeParser.h>

#include "naucrates/dxl/CDXLUtils.h"

namespace DB
{

	TypeProvider::TYPE_OID_ID = 3;
	std::pair<Oid, IMDTypePtr> TypeProvider::TYPE_FLOAT32 = {Oid(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
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
	std::pair<Oid, IMDTypePtr> TYPE_FLOAT64 = {Oid(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(Oid(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Float64)), 
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
	std::pair<Oid, IMDTypePtr> TYPE_BOOLEAN = {Oid(++TypeProvider::TYPE_OID_ID), GPOS_NEW(mp) gpmd::CMDTypeGenericGPDB(
			mp, gpmd::CMDIdGPDB(Oid(TypeProvider::TYPE_OID_ID)), /*mdname*/ CreateMDName(getTypeName(TypeIndex::Int8)), 
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
};

PGTypePtr TypeProvider::getTypeByOid(Oid oid)
{
	//TODO
	return std::make_shared<Form_pg_type>();
};

/*
 * getBaseType
 *		If the given type is a domain, return its base type;
 *		otherwise return the type's own OID.
 */
Oid TypeProvider::getBaseType(Oid typid)
{
    int32 typmod = -1;

    return getBaseTypeAndTypmod(typid, &typmod);
};

/*
 * getBaseTypeAndTypmod
 *		If the given type is a domain, return its base type and typmod;
 *		otherwise return the type's own OID, and leave *typmod unchanged.
 *
 * Note that the "applied typmod" should be -1 for every domain level
 * above the bottommost; therefore, if the passed-in typid is indeed
 * a domain, *typmod should be -1.
 */
Oid TypeProvider::getBaseTypeAndTypmod(Oid typid, int32 * typmod)
{
	/*
	 * We loop to find the bottom base type in a stack of domains.
	 */
    for (;;)
    {
		PGTypePtr tup = getTypeByOid(typid);
		if (tup == NULL)
		{
			elog(ERROR, "cache lookup failed for type %u", typid);
		}

		if (tup->typtype != TYPTYPE_DOMAIN)
		{
			break;
		}

		Assert(*typmod == -1);
		typid = typTup->typbasetype;
		*typmod = typTup->typtypmod;

    }

    return typid;
};

/*
 * get_type_category_preferred
 *
 *		Given the type OID, fetch its category and preferred-type status.
 *		Throws error on failure.
 */
void TypeProvider::get_type_category_preferred(Oid typid, char * typcategory, bool * typispreferred)
{
	PGTypePtr tup = getTypeByOid(typid);
	if (tup == NULL)
	{
		elog(ERROR, "cache lookup failed for type %u", typid);
	}
	*typcategory = tup->typcategory;
    *typispreferred = tup->typispreferred;
};

char * TypeProvider::format_type_be(Oid type_oid)
{
    return format_type_internal(type_oid, -1, false, false, false);
};

char * TypeProvider::printTypmod(const char * typname, int32 typmod, Oid typmodout)
{
    char * res;

    /* Shouldn't be called if typmod is -1 */
    Assert(typmod >= 0)

    if (typmodout == InvalidOid)
    {
        /* Default behavior: just print the integer typmod with parens */
        res = psprintf("%s(%d)", typname, (int)typmod);
    }
    else
    {
        /* Use the type-specific typmodout procedure */
        char * tmstr;

        tmstr = DatumGetCString(function_provider->OidFunctionCall1(typmodout, Int32GetDatum(typmod)));
        res = psprintf("%s%s", typname, tmstr);
    }

    return res;
};

bool TypeProvider::TypeIsVisible(Oid typid)
{
    //HeapTuple typtup;
    //Form_pg_type typform;
    Oid typnamespace;
    bool visible;

    PGTypePtr typtup = getTypeByOid(typid);
    if (typtup == NULL)
        elog(ERROR, "cache lookup failed for type %u", typid);

    recomputeNamespacePath();

    /*
	 * Quick check: if it ain't in the path at all, it ain't visible. Items in
	 * the system namespace are surely in the path and so we needn't even do
	 * list_member_oid() for them.
	 */
    typnamespace = typtup->typnamespace;
    if (typnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(activeSearchPath, typnamespace))
        visible = false;
    else
    {
        /*
		 * If it is in the path, it might still not be visible; it could be
		 * hidden by another type of the same name earlier in the path. So we
		 * must do a slow check for conflicting types.
		 */
        char * typname = NameStr(typtup->typname);
        PGListCell * l;

        visible = false;
        foreach (l, activeSearchPath)
        {
            Oid namespaceId = lfirst_oid(l);

            if (namespaceId == typnamespace)
            {
                /* Found it first in path */
                visible = true;
                break;
            }
            if (SearchSysCacheExists2(TYPENAMENSP, PointerGetDatum(typname), ObjectIdGetDatum(namespaceId)))
            {
                /* Found something else first in path */
                break;
            }
        }
    }

    return visible;
};

char * TypeProvider::format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify)
{
    bool with_typemod = typemod_given && (typemod >= 0);
    PGTypePtr tuple;
    //Form_pg_type typeform;
    Oid array_base_type;
    bool is_array;
    char * buf;

    if (type_oid == InvalidOid && allow_invalid)
        return pstrdup("-");

    PGTypePtr tuple = getTypeByOid(type_oid);
    if (tuple == NULL)
    {
        if (allow_invalid)
            return pstrdup("???");
        else
            elog(ERROR, "cache lookup failed for type %u", type_oid);
    }

    /*
	 * Check if it's a regular (variable length) array type.  Fixed-length
	 * array types such as "name" shouldn't get deconstructed.  As of Postgres
	 * 8.1, rather than checking typlen we check the toast property, and don't
	 * deconstruct "plain storage" array types --- this is because we don't
	 * want to show oidvector as oid[].
	 */
    array_base_type = tuple->typelem;

    if (array_base_type != InvalidOid && tuple->typstorage != 'p')
    {
        /* Switch our attention to the array element type */
        ReleaseSysCache(tuple);
        tuple = getTypeByOid(array_base_type);
        if (tuple == NULL)
        {
            if (allow_invalid)
                return pstrdup("???[]");
            else
                elog(ERROR, "cache lookup failed for type %u", type_oid);
        }
        type_oid = array_base_type;
        is_array = true;
    }
    else
        is_array = false;

    /*
	 * See if we want to special-case the output for certain built-in types.
	 * Note that these special cases should all correspond to special
	 * productions in gram.y, to ensure that the type name will be taken as a
	 * system type, not a user type of the same name.
	 *
	 * If we do not provide a special-case output here, the type name will be
	 * handled the same way as a user type name --- in particular, it will be
	 * double-quoted if it matches any lexer keyword.  This behavior is
	 * essential for some cases, such as types "bit" and "char".
	 */
    buf = NULL; /* flag for no special case */

    switch (type_oid)
    {
        case BITOID:
            if (with_typemod)
                buf = printTypmod("bit", typemod, typeform->typmodout);
            else if (typemod_given)
            {
                /*
				 * bit with typmod -1 is not the same as BIT, which means
				 * BIT(1) per SQL spec.  Report it as the quoted typename so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = pstrdup("bit");
            break;

        case BOOLOID:
            buf = pstrdup("boolean");
            break;

        case BPCHAROID:
            if (with_typemod)
                buf = printTypmod("character", typemod, typeform->typmodout);
            else if (typemod_given)
            {
                /*
				 * bpchar with typmod -1 is not the same as CHARACTER, which
				 * means CHARACTER(1) per SQL spec.  Report it as bpchar so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = pstrdup("character");
            break;

        case FLOAT4OID:
            buf = pstrdup("real");
            break;

        case FLOAT8OID:
            buf = pstrdup("double precision");
            break;

        case INT2OID:
            buf = pstrdup("smallint");
            break;

        case INT4OID:
            buf = pstrdup("integer");
            break;

        case INT8OID:
            buf = pstrdup("bigint");
            break;

        case NUMERICOID:
            if (with_typemod)
                buf = printTypmod("numeric", typemod, typeform->typmodout);
            else
                buf = pstrdup("numeric");
            break;

        case INTERVALOID:
            if (with_typemod)
                buf = printTypmod("interval", typemod, typeform->typmodout);
            else
                buf = pstrdup("interval");
            break;

        case TIMEOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, typeform->typmodout);
            else
                buf = pstrdup("time without time zone");
            break;

        case TIMETZOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, typeform->typmodout);
            else
                buf = pstrdup("time with time zone");
            break;

        case TIMESTAMPOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, typeform->typmodout);
            else
                buf = pstrdup("timestamp without time zone");
            break;

        case TIMESTAMPTZOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, typeform->typmodout);
            else
                buf = pstrdup("timestamp with time zone");
            break;

        case VARBITOID:
            if (with_typemod)
                buf = printTypmod("bit varying", typemod, typeform->typmodout);
            else
                buf = pstrdup("bit varying");
            break;

        case VARCHAROID:
            if (with_typemod)
                buf = printTypmod("character varying", typemod, typeform->typmodout);
            else
                buf = pstrdup("character varying");
            break;
    }

    if (buf == NULL)
    {
        /*
		 * Default handling: report the name as it appears in the catalog.
		 * Here, we must qualify the name if it is not visible in the search
		 * path, and we must double-quote it if it's not a standard identifier
		 * or if it matches any keyword.
		 */
        char * nspname;
        char * typname;

        if (!force_qualify && TypeIsVisible(type_oid))
            nspname = NULL;
        else
            nspname = get_namespace_name(typeform->typnamespace);

        typname = NameStr(typeform->typname);

        buf = quote_qualified_identifier(nspname, typname);

        if (with_typemod)
            buf = printTypmod(buf, typemod, typeform->typmodout);
    }

    if (is_array)
        buf = psprintf("%s[]", buf);

    ReleaseSysCache(tuple);

    return buf;
};

TupleDesc TypeProvider::lookup_rowtype_tupdesc(Oid type_id, int32 typmod)
{
    TupleDesc tupDesc;

    tupDesc = lookup_rowtype_tupdesc_internal(type_id, typmod, false);
    IncrTupleDescRefCount(tupDesc);
    return tupDesc;
};

Oid TypeProvider::get_element_type(Oid typid)
{
	PGTypePtr tup = getTypeByOid(typid);

    if (tup != NULL)
    {
        Oid result;

        if (tup->typlen == -1)
            result = tup->typelem;
        else
            result = InvalidOid;

        return result;
    }
    else
        return InvalidOid;
};

void TypeProvider::getTypeOutputInfo(Oid type, Oid * typOutput, bool * typIsVarlena)
{
    HeapTuple typeTuple;
    Form_pg_type pt;

    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", type);
    pt = (Form_pg_type)GETSTRUCT(typeTuple);

    if (!pt->typisdefined)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    if (!OidIsValid(pt->typoutput))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("no output function available for type %s", format_type_be(type))));

    *typOutput = pt->typoutput;
    *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);

    ReleaseSysCache(typeTuple);
};

void TypeProvider::getTypeInputInfo(Oid type, Oid * typInput, Oid * typIOParam)
{
    HeapTuple typeTuple;
    Form_pg_type pt;

    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", type);
    pt = (Form_pg_type)GETSTRUCT(typeTuple);

    if (!pt->typisdefined)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    if (!OidIsValid(pt->typinput))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("no input function available for type %s", format_type_be(type))));

    *typInput = pt->typinput;
    *typIOParam = getTypeIOParam(typeTuple);

    ReleaseSysCache(typeTuple);
};

bool TypeProvider::typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId)
{
    bool result = false;
    Oid subclassRelid;
    Oid superclassRelid;
    Relation inhrel;
    PGList *visited, *queue;
    PGListCell * queue_item;

    /* We need to work with the associated relation OIDs */
    subclassRelid = type_parser->typeidTypeRelid(subclassTypeId);
    if (subclassRelid == InvalidOid)
        return false; /* not a complex type */
    superclassRelid = type_parser->typeidTypeRelid(superclassTypeId);
    if (superclassRelid == InvalidOid)
        return false; /* not a complex type */

    /* No point in searching if the superclass has no subclasses */
    if (!has_subclass(superclassRelid))
        return false;

    /*
	 * Begin the search at the relation itself, so add its relid to the queue.
	 */
    queue = list_make1_oid(subclassRelid);
    visited = NIL;

    inhrel = heap_open(InheritsRelationId, AccessShareLock);

    /*
	 * Use queue to do a breadth-first traversal of the inheritance graph from
	 * the relid supplied up to the root.  Notice that we append to the queue
	 * inside the loop --- this is okay because the foreach() macro doesn't
	 * advance queue_item until the next loop iteration begins.
	 */
    foreach (queue_item, queue)
    {
        Oid this_relid = lfirst_oid(queue_item);
        ScanKeyData skey;
        SysScanDesc inhscan;
        HeapTuple inhtup;

        /*
		 * If we've seen this relid already, skip it.  This avoids extra work
		 * in multiple-inheritance scenarios, and also protects us from an
		 * infinite loop in case there is a cycle in pg_inherits (though
		 * theoretically that shouldn't happen).
		 */
        if (list_member_oid(visited, this_relid))
            continue;

        /*
		 * Okay, this is a not-yet-seen relid. Add it to the list of
		 * already-visited OIDs, then find all the types this relid inherits
		 * from and add them to the queue.
		 */
        visited = lappend_oid(visited, this_relid);

        ScanKeyInit(&skey, Anum_pg_inherits_inhrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(this_relid));

        inhscan = systable_beginscan(inhrel, InheritsRelidSeqnoIndexId, true, NULL, 1, &skey);

        while ((inhtup = systable_getnext(inhscan)) != NULL)
        {
            Form_pg_inherits inh = (Form_pg_inherits)GETSTRUCT(inhtup);
            Oid inhparent = inh->inhparent;

            /* If this is the target superclass, we're done */
            if (inhparent == superclassRelid)
            {
                result = true;
                break;
            }

            /* Else add to queue */
            queue = lappend_oid(queue, inhparent);
        }

        systable_endscan(inhscan);

        if (result)
            break;
    }

    /* clean up ... */
    heap_close(inhrel, AccessShareLock);

    list_free(visited);
    list_free(queue);

    return result;
};

}
