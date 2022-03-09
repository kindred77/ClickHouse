/*
 * MDProviderRelcache.cpp
 *
 *  Created on: 2022Äê3ÔÂ1ÈÕ
 *      Author: tangye
 */

#include "Interpreters/orcaopt/MDProviderRelcache.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderRelcache::CMDProviderRelcache
//
//	@doc:
//		Constructs a file-based metadata provider
//
//---------------------------------------------------------------------------
MDProviderRelcache::MDProviderRelcache(CMemoryPool *mp) : m_mp(mp)
{
	GPOS_ASSERT(NULL != m_mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderRelcache::GetMDObjDXLStr
//
//	@doc:
//		Returns the DXL of the requested object in the provided memory pool
//
//---------------------------------------------------------------------------
CWStringBase *
MDProviderRelcache::GetMDObjDXLStr(CMemoryPool *mp, CMDAccessor *md_accessor,
									IMDId *md_id) const
{
	IMDCacheObject *md_obj =
		TranslatorRelcacheToDXL::RetrieveObject(mp, md_accessor, md_id);

	GPOS_ASSERT(NULL != md_obj);

	CWStringDynamic *str = CDXLUtils::SerializeMDObj(
		m_mp, md_obj, true /*fSerializeHeaders*/, false /*findent*/);

	// cleanup DXL object
	md_obj->Release();

	return str;
}

// EOF


