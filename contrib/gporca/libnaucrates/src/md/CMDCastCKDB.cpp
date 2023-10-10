#include "naucrates/md/CMDCastCKDB.h"

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpmd;
using namespace gpdxl;

CMDCastCKDB::CMDCastCKDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname,
						 IMDId *mdid_src, IMDId *mdid_dest,
						 BOOL is_binary_coercible, IMDId *mdid_cast_func,
						 EmdCoercepathType path_type)
	: m_mp(mp),
	m_mdid(mdid),
	m_mdname(mdname),
	m_mdid_src(mdid_src),
	m_mdid_dest(mdid_dest),
	m_is_binary_coercible(is_binary_coercible),
	m_mdid_cast_func(mdid_cast_func),
	m_path_type(path_type)
{
	GPOS_ASSERT(m_mdid->IsValid());
	GPOS_ASSERT(m_mdid_src->IsValid());
	GPOS_ASSERT(m_mdid_dest->IsValid());
	GPOS_ASSERT_IMP(!is_binary_coercible && m_path_type != EmdtCoerceViaIO,
					m_mdid_cast_func->IsValid());

	m_dxl_str = CDXLUtils::SerializeMDObj(
		m_mp, this, false /*fSerializeHeader*/, false /*indentation*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::~CMDCastCKDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDCastCKDB::~CMDCastCKDB()
{
	m_mdid->Release();
	m_mdid_src->Release();
	m_mdid_dest->Release();
	CRefCount::SafeRelease(m_mdid_cast_func);
	GPOS_DELETE(m_mdname);
	GPOS_DELETE(m_dxl_str);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::MDId
//
//	@doc:
//		Mdid of cast object
//
//---------------------------------------------------------------------------
IMDId *
CMDCastCKDB::MDId() const
{
	return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::Mdname
//
//	@doc:
//		Func name
//
//---------------------------------------------------------------------------
CMDName
CMDCastCKDB::Mdname() const
{
	return *m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::MdidSrc
//
//	@doc:
//		Source type id
//
//---------------------------------------------------------------------------
IMDId *
CMDCastCKDB::MdidSrc() const
{
	return m_mdid_src;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::MdidDest
//
//	@doc:
//		Destination type id
//
//---------------------------------------------------------------------------
IMDId *
CMDCastCKDB::MdidDest() const
{
	return m_mdid_dest;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::GetCastFuncMdId
//
//	@doc:
//		Cast function id
//
//---------------------------------------------------------------------------
IMDId *
CMDCastCKDB::GetCastFuncMdId() const
{
	return m_mdid_cast_func;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::IsBinaryCoercible
//
//	@doc:
//		Returns whether this is a cast between binary coercible types, i.e. the
//		types are binary compatible
//
//---------------------------------------------------------------------------
BOOL
CMDCastCKDB::IsBinaryCoercible() const
{
	return m_is_binary_coercible;
}

// returns coercion path type
IMDCast::EmdCoercepathType
CMDCastCKDB::GetMDPathType() const
{
	return m_path_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::Serialize
//
//	@doc:
//		Serialize function metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDCastCKDB::Serialize(CXMLSerializer *xml_serializer) const
{
	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBCast));

	m_mdid->Serialize(xml_serializer,
					  CDXLTokens::GetDXLTokenStr(EdxltokenMdid));

	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenName),
								 m_mdname->GetMDName());

	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBCastBinaryCoercible),
		m_is_binary_coercible);
	m_mdid_src->Serialize(xml_serializer,
						  CDXLTokens::GetDXLTokenStr(EdxltokenGPDBCastSrcType));
	m_mdid_dest->Serialize(
		xml_serializer, CDXLTokens::GetDXLTokenStr(EdxltokenGPDBCastDestType));
	m_mdid_cast_func->Serialize(
		xml_serializer, CDXLTokens::GetDXLTokenStr(EdxltokenGPDBCastFuncId));
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBCastCoercePathType),
		m_path_type);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenGPDBCast));
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDCastCKDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDCastCKDB::DebugPrint(IOstream &os) const
{
	os << "Cast " << (Mdname()).GetMDName()->GetBuffer() << ": ";
	MdidSrc()->OsPrint(os);
	os << "->";
	MdidDest()->OsPrint(os);
	os << std::endl;

	if (m_is_binary_coercible)
	{
		os << ", binary-coercible";
	}

	if (IMDId::IsValid(m_mdid_cast_func))
	{
		os << ", Cast func id: ";
		GetCastFuncMdId()->OsPrint(os);
	}

	os << std::endl;
}

#endif	// GPOS_DEBUG

// EOF
