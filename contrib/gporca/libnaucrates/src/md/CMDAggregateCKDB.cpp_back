#include "naucrates/md/CMDAggregateCKDB.h"

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpmd;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::CMDAggregateCKDB
//
//	@doc:
//		Constructs a metadata aggregate
//
//---------------------------------------------------------------------------
CMDAggregateCKDB::CMDAggregateCKDB(CMemoryPool *mp, IMDId *mdid,
								   CMDName *mdname, IMDId *result_type_mdid,
								   IMDId *intermediate_result_type_mdid,
								   BOOL fOrdered, BOOL is_splittable,
								   BOOL is_hash_agg_capable)
	: m_mp(mp),
	m_mdid(mdid),
	m_mdname(mdname),
	m_mdid_type_result(result_type_mdid),
	m_mdid_type_intermediate(intermediate_result_type_mdid),
	m_is_ordered(fOrdered),
	m_is_splittable(is_splittable),
	m_hash_agg_capable(is_hash_agg_capable)
{
	GPOS_ASSERT(mdid->IsValid());

	m_dxl_str = CDXLUtils::SerializeMDObj(
		m_mp, this, false /*fSerializeHeader*/, false /*indentation*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::~CMDAggregateCKDB
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CMDAggregateCKDB::~CMDAggregateCKDB()
{
	m_mdid->Release();
	m_mdid_type_intermediate->Release();
	m_mdid_type_result->Release();
	GPOS_DELETE(m_mdname);
	GPOS_DELETE(m_dxl_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::MDId
//
//	@doc:
//		Agg id
//
//---------------------------------------------------------------------------
IMDId *
CMDAggregateCKDB::MDId() const
{
	return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::Mdname
//
//	@doc:
//		Agg name
//
//---------------------------------------------------------------------------
CMDName
CMDAggregateCKDB::Mdname() const
{
	return *m_mdname;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::GetResultTypeMdid
//
//	@doc:
//		Type id of result
//
//---------------------------------------------------------------------------
IMDId *
CMDAggregateCKDB::GetResultTypeMdid() const
{
	return m_mdid_type_result;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::GetIntermediateResultTypeMdid
//
//	@doc:
//		Type id of intermediate result
//
//---------------------------------------------------------------------------
IMDId *
CMDAggregateCKDB::GetIntermediateResultTypeMdid() const
{
	return m_mdid_type_intermediate;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::Serialize
//
//	@doc:
//		Serialize function metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDAggregateCKDB::Serialize(CXMLSerializer *xml_serializer) const
{
	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBAgg));

	m_mdid->Serialize(xml_serializer,
					  CDXLTokens::GetDXLTokenStr(EdxltokenMdid));

	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenName),
								 m_mdname->GetMDName());
	if (m_is_ordered)
	{
		xml_serializer->AddAttribute(
			CDXLTokens::GetDXLTokenStr(EdxltokenGPDBIsAggOrdered),
			m_is_ordered);
	}

	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBAggSplittable),
		m_is_splittable);
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBAggHashAggCapable),
		m_hash_agg_capable);

	SerializeMDIdAsElem(
		xml_serializer,
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBAggResultTypeId),
		m_mdid_type_result);
	SerializeMDIdAsElem(
		xml_serializer,
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBAggIntermediateResultTypeId),
		m_mdid_type_intermediate);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBAgg));
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateCKDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDAggregateCKDB::DebugPrint(IOstream &os) const
{
	os << "Aggregate id: ";
	MDId()->OsPrint(os);
	os << std::endl;

	os << "Aggregate name: " << (Mdname()).GetMDName()->GetBuffer()
	   << std::endl;

	os << "Result type id: ";
	GetResultTypeMdid()->OsPrint(os);
	os << std::endl;

	os << "Intermediate result type id: ";
	GetIntermediateResultTypeMdid()->OsPrint(os);
	os << std::endl;

	os << std::endl;
}

#endif	// GPOS_DEBUG

// EOF
