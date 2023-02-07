/*
 * TranslatorRelcacheToDXL.cpp
 *
 *  Created on: 2022Äê3ÔÂ1ÈÕ
 *      Author: tangye
 */
#include "Interpreters/orcaopt/TranslatorRelcacheToDXL.h"

#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/io/COstreamString.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/exception.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/md/CMDArrayCoerceCastGPDB.h"
#include "naucrates/md/CMDCastGPDB.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"
#include "naucrates/md/CMDScCmpGPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeOidGPDB.h"

#include <Storages/IStorage.h>

using namespace gpdxl;
using namespace gpopt;


static const ULONG cmp_type_mappings[][2] = {
	{IMDType::EcmptEq, CmptEq},	  {IMDType::EcmptNEq, CmptNEq},
	{IMDType::EcmptL, CmptLT},	  {IMDType::EcmptG, CmptGT},
	{IMDType::EcmptGEq, CmptGEq}, {IMDType::EcmptLEq, CmptLEq}};


TranslatorRelcacheToDXL::TranslatorRelcacheToDXL(ContextPtr context_)
	: context(context_),
	  storage_provider(context_)
{

}
//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveObject
//
//	@doc:
//		Retrieve a metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
TranslatorRelcacheToDXL::RetrieveObject(CMemoryPool *mp,
										 CMDAccessor *md_accessor, IMDId *mdid)
{
	IMDCacheObject *md_obj = NULL;
	GPOS_ASSERT(NULL != md_accessor);

#ifdef FAULT_INJECTOR
	gpdb::InjectFaultInOptTasks("opt_relcache_translator_catalog_access");
#endif	// FAULT_INJECTOR

	switch (mdid->MdidType())
	{
		case IMDId::EmdidGPDB:
			md_obj = RetrieveObjectCK(mp, md_accessor, mdid);
			break;

		case IMDId::EmdidRelStats:
			md_obj = RetrieveRelStats(mp, mdid);
			break;

		case IMDId::EmdidColStats:
			md_obj = RetrieveColStats(mp, md_accessor, mdid);
			break;

		case IMDId::EmdidCastFunc:
			md_obj = RetrieveCast(mp, mdid);
			break;

		case IMDId::EmdidScCmp:
			md_obj = RetrieveScCmp(mp, mdid);
			break;

		default:
			break;
	}

	if (NULL == md_obj)
	{
		// no match found
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return md_obj;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveMDObjGPDB
//
//	@doc:
//		Retrieve a GPDB metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
TranslatorRelcacheToDXL::RetrieveObjectCK(CMemoryPool *mp,
											 CMDAccessor *md_accessor,
											 IMDId *mdid)
{
	GPOS_ASSERT(mdid->MdidType() == CMDIdGPDB::EmdidGPDB);

	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(0 != oid);

	// find out what type of object this oid stands for

	if (gpdb::TypeExists(oid))
	{
		return RetrieveType(mp, mdid);
	}

	if (gpdb::RelationExists(oid))
	{
		return RetrieveRel(mp, md_accessor, mdid);
	}

	if (gpdb::OperatorExists(oid))
	{
		return RetrieveScOp(mp, mdid);
	}

	if (gpdb::AggregateExists(oid))
	{
		return RetrieveAgg(mp, mdid);
	}

	if (gpdb::FunctionExists(oid))
	{
		return RetrieveFunc(mp, mdid);
	}

	// no match found
	return NULL;
}

IMDType *
TranslatorRelcacheToDXL::RetrieveType(CMemoryPool *mp, IMDId *mdid)
{

}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetRelName
//
//	@doc:
//		Return a relation name
//
//---------------------------------------------------------------------------
CMDName *
TranslatorRelcacheToDXL::GetRelName(CMemoryPool *mp, DB::StoragePtr storage)
{
	GPOS_ASSERT(storage);
	CWStringDynamic *relname_str =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, storage->getTableName().c_str());
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, relname_str);
	GPOS_DELETE(relname_str);
	return mdname;
}

CMDColumnArray *
TranslatorRelcacheToDXL::RetrieveRelColumns(
	CMemoryPool *mp, CMDAccessor *md_accessor, DB::StoragePtr storage,
	IMDRelation::Erelstoragetype rel_storage_type)
{
	CMDColumnArray *mdcol_array = GPOS_NEW(mp) CMDColumnArray(mp);
	for(auto name : storage->getInMemoryMetadataPtr()->getColumns().getNamesOfPhysical())
	{
		CMDName *md_colname =
				CDXLUtils::CreateMDNameFromCharArray(mp, name.c_str());
		//get default value

	}
}

IMDRelation *
TranslatorRelcacheToDXL::RetrieveRel(CMemoryPool *mp, CMDAccessor *md_accessor,
									IMDId *mdid)
{
	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != oid);

	DB::StoragePtr storage = storage_provider->getStorageByOID(oid);
	if (!storage)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	CMDName *mdname = GetRelName(mp, storage);
	IMDRelation::Erelstoragetype rel_storage_type =
			IMDRelation::ErelstorageAppendOnlyCols;
	CMDColumnArray *mdcol_array = RetrieveRelColumns(mp, md_accessor, storage, rel_storage_type);
}



