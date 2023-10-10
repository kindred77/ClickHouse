#include "naucrates/md/CMDTypeInt4CKDB.h"

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDIdCKDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLScalarConstValue.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

// static member initialization
CWStringConst CMDTypeInt4CKDB::m_str = CWStringConst(GPOS_WSZ_LIT("int4"));
CMDName CMDTypeInt4CKDB::m_mdname(&m_str);

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::CMDTypeInt4CKDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTypeInt4CKDB::CMDTypeInt4CKDB(CMemoryPool *mp) : m_mp(mp)
{
	m_mdid = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeType, GPDB_INT4_OID);
	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		m_distr_opfamily = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_OPFAMILY);
		m_legacy_distr_opfamily =
			GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_LEGACY_OPFAMILY);
	}
	else
	{
		m_distr_opfamily = NULL;
		m_legacy_distr_opfamily = NULL;
	}
	m_mdid_op_eq = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_EQ_OP);
	m_mdid_op_neq = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_NEQ_OP);
	m_mdid_op_lt = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_LT_OP);
	m_mdid_op_leq = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_LEQ_OP);
	m_mdid_op_gt = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_GT_OP);
	m_mdid_op_geq = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_GEQ_OP);
	m_mdid_op_cmp = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeOperator, GPDB_INT4_COMP_OP);
	m_mdid_type_array = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeType, GPDB_INT4_ARRAY_TYPE);
	m_mdid_min = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeAggregate, GPDB_INT4_AGG_MIN);
	m_mdid_max = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeAggregate, GPDB_INT4_AGG_MAX);
	m_mdid_avg = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeAggregate, GPDB_INT4_AGG_AVG);
	m_mdid_sum = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeAggregate, GPDB_INT4_AGG_SUM);
	m_mdid_count = GPOS_NEW(mp) CMDIdCKDB(CMDIdCKDB::ECKDBMDOIdType::ECKDBMDOIdTypeAggregate, GPDB_INT4_AGG_COUNT);

	m_dxl_str = CDXLUtils::SerializeMDObj(
		m_mp, this, false /*fSerializeHeader*/, false /*indentation*/);

	GPOS_ASSERT(GPDB_INT4_OID == CMDIdGPDB::CastMdid(m_mdid)->Oid());
	m_mdid->AddRef();
	m_datum_null =
		GPOS_NEW(mp) CDatumInt4GPDB(m_mdid, 1 /* value */, true /* is_null */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::~CMDTypeInt4CKDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTypeInt4CKDB::~CMDTypeInt4CKDB()
{
	m_mdid->Release();
	CRefCount::SafeRelease(m_distr_opfamily);
	CRefCount::SafeRelease(m_legacy_distr_opfamily);
	m_mdid_op_eq->Release();
	m_mdid_op_neq->Release();
	m_mdid_op_lt->Release();
	m_mdid_op_leq->Release();
	m_mdid_op_gt->Release();
	m_mdid_op_geq->Release();
	m_mdid_op_cmp->Release();
	m_mdid_type_array->Release();
	m_mdid_min->Release();
	m_mdid_max->Release();
	m_mdid_avg->Release();
	m_mdid_sum->Release();
	m_mdid_count->Release();
	m_datum_null->Release();

	GPOS_DELETE(m_dxl_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetDatum
//
//	@doc:
//		Factory function for creating INT4 datums
//
//---------------------------------------------------------------------------
IDatumInt4 *
CMDTypeInt4CKDB::CreateInt4Datum(CMemoryPool *mp, INT value, BOOL is_null) const
{
	return GPOS_NEW(mp) CDatumInt4GPDB(m_mdid->Sysid(), value, is_null);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::MDId
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt4CKDB::MDId() const
{
	return m_mdid;
}

IMDId *
CMDTypeInt4CKDB::GetDistrOpfamilyMdid() const
{
	if (GPOS_FTRACE(EopttraceUseLegacyOpfamilies))
	{
		return m_legacy_distr_opfamily;
	}
	else
	{
		return m_distr_opfamily;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::Mdname
//
//	@doc:
//		Returns the name of this type
//
//---------------------------------------------------------------------------
CMDName
CMDTypeInt4CKDB::Mdname() const
{
	return CMDTypeInt4CKDB::m_mdname;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetMdidForCmpType
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt4CKDB::GetMdidForCmpType(ECmpType cmp_type) const
{
	switch (cmp_type)
	{
		case EcmptEq:
			return m_mdid_op_eq;
		case EcmptNEq:
			return m_mdid_op_neq;
		case EcmptL:
			return m_mdid_op_lt;
		case EcmptLEq:
			return m_mdid_op_leq;
		case EcmptG:
			return m_mdid_op_gt;
		case EcmptGEq:
			return m_mdid_op_geq;
		default:
			GPOS_ASSERT(!"Invalid operator type");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetMdidForAggType
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt4CKDB::GetMdidForAggType(EAggType agg_type) const
{
	switch (agg_type)
	{
		case EaggMin:
			return m_mdid_min;
		case EaggMax:
			return m_mdid_max;
		case EaggAvg:
			return m_mdid_avg;
		case EaggSum:
			return m_mdid_sum;
		case EaggCount:
			return m_mdid_count;
		default:
			GPOS_ASSERT(!"Invalid aggregate type");
			return NULL;
	}
}
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTypeInt4CKDB::Serialize(CXMLSerializer *xml_serializer) const
{
	CGPDBTypeHelper<CMDTypeInt4CKDB>::Serialize(xml_serializer, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetDatumForDXLConstVal
//
//	@doc:
//		Transformation method for generating int4 datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum *
CMDTypeInt4CKDB::GetDatumForDXLConstVal(
	const CDXLScalarConstValue *dxl_op) const
{
	CDXLDatumInt4 *dxl_datum =
		CDXLDatumInt4::Cast(const_cast<CDXLDatum *>(dxl_op->GetDatumVal()));

	return GPOS_NEW(m_mp) CDatumInt4GPDB(m_mdid->Sysid(), dxl_datum->Value(),
										 dxl_datum->IsNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetDatumForDXLDatum
//
//	@doc:
//		Construct an int4 datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum *
CMDTypeInt4CKDB::GetDatumForDXLDatum(CMemoryPool *mp,
									 const CDXLDatum *dxl_datum) const
{
	CDXLDatumInt4 *int4_dxl_datum =
		CDXLDatumInt4::Cast(const_cast<CDXLDatum *>(dxl_datum));
	INT val = int4_dxl_datum->Value();
	BOOL is_null = int4_dxl_datum->IsNull();

	return GPOS_NEW(mp) CDatumInt4GPDB(m_mdid->Sysid(), val, is_null);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetDatumVal
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt4CKDB::GetDatumVal(CMemoryPool *mp, IDatum *datum) const
{
	m_mdid->AddRef();
	CDatumInt4GPDB *int4_datum = dynamic_cast<CDatumInt4GPDB *>(datum);

	return GPOS_NEW(mp)
		CDXLDatumInt4(mp, m_mdid, int4_datum->IsNull(), int4_datum->Value());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetDXLOpScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeInt4CKDB::GetDXLOpScConst(CMemoryPool *mp, IDatum *datum) const
{
	CDatumInt4GPDB *int4gpdb_datum = dynamic_cast<CDatumInt4GPDB *>(datum);

	m_mdid->AddRef();
	CDXLDatumInt4 *dxl_datum = GPOS_NEW(mp) CDXLDatumInt4(
		mp, m_mdid, int4gpdb_datum->IsNull(), int4gpdb_datum->Value());

	return GPOS_NEW(mp) CDXLScalarConstValue(mp, dxl_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::GetDXLDatumNull
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt4CKDB::GetDXLDatumNull(CMemoryPool *mp) const
{
	m_mdid->AddRef();

	return GPOS_NEW(mp) CDXLDatumInt4(mp, m_mdid, true /*is_null*/, 1);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4CKDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTypeInt4CKDB::DebugPrint(IOstream &os) const
{
	CGPDBTypeHelper<CMDTypeInt4CKDB>::DebugPrint(os, this);
}

#endif	// GPOS_DEBUG

// EOF
