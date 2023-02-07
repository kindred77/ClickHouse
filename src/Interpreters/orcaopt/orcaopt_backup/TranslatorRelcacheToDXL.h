#pragma once

#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/md/CMDAggregateGPDB.h"
#include "naucrates/md/CMDCheckConstraintGPDB.h"
#include "naucrates/md/CMDFunctionGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"
#include "naucrates/md/CMDRelationExternalGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDScalarOpGPDB.h"
#include "naucrates/md/CMDTriggerGPDB.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatisticsUtils.h"

#include <Interpreters/orcaopt/StorageProvider.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

struct LogicalIndexes;
struct LogicalIndexInfo;

namespace gpdxl
{

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CTranslatorRelcacheToDXL
//
//	@doc:
//		Class for translating GPDB's relcache entries into DXL MD objects
//
//---------------------------------------------------------------------------
class TranslatorRelcacheToDXL
{
private:

	StorageProviderPtr storage_provider;
	ContextPtr context;

	CMDName *GetRelName(CMemoryPool *mp, DB::StoragePtr storage);
	// hash map structure to store gpdb att -> opt col information
	typedef CHashMap<CGPDBAttInfo, CGPDBAttOptCol, HashGPDBAttInfo,
					EqualGPDBAttInfo, CleanupRelease, CleanupRelease>
			GPDBAttOptColHashMap;

	// iterator
	typedef CHashMapIter<CGPDBAttInfo, CGPDBAttOptCol, HashGPDBAttInfo,
						EqualGPDBAttInfo, CleanupRelease, CleanupRelease>
			GPDBAttOptColHashMapIter;

	CMDColumnArray *RetrieveRelColumns(
		CMemoryPool *mp, CMDAccessor *md_accessor, DB::StoragePtr storage,
		IMDRelation::Erelstoragetype rel_storage_type);

	// retrieve a GPDB metadata object from the relcache
	IMDCacheObject *RetrieveObjectCK(CMemoryPool *mp,
											  CMDAccessor *md_accessor,
											  IMDId *mdid);

	// retrieve relstats object from the relcache
	IMDCacheObject *RetrieveRelStats(CMemoryPool *mp, IMDId *mdid);

	// retrieve column stats object from the relcache
	IMDCacheObject *RetrieveColStats(CMemoryPool *mp,
											CMDAccessor *md_accessor,
											IMDId *mdid);

	// retrieve cast object from the relcache
	IMDCacheObject *RetrieveCast(CMemoryPool *mp, IMDId *mdid);

	// retrieve scalar comparison object from the relcache
	IMDCacheObject *RetrieveScCmp(CMemoryPool *mp, IMDId *mdid);

public:
	TranslatorRelcacheToDXL(ContextPtr context_);
	// retrieve a metadata object from the relcache
	IMDCacheObject *RetrieveObject(CMemoryPool *mp,
										  CMDAccessor *md_accessor,
										  IMDId *mdid);

	// retrieve a relation from the relcache
	IMDRelation *RetrieveRel(CMemoryPool *mp, CMDAccessor *md_accessor,
									IMDId *mdid);

	// retrieve a type from the relcache
	IMDType *RetrieveType(CMemoryPool *mp, IMDId *mdid);

	// retrieve a scalar operator from the relcache
	CMDScalarOpGPDB *RetrieveScOp(CMemoryPool *mp, IMDId *mdid);

	// retrieve a function from the relcache
	CMDFunctionGPDB *RetrieveFunc(CMemoryPool *mp, IMDId *mdid);

	// retrieve an aggregate from the relcache
	CMDAggregateGPDB *RetrieveAgg(CMemoryPool *mp, IMDId *mdid);

	// retrieve a trigger from the relcache
	CMDTriggerGPDB *RetrieveTrigger(CMemoryPool *mp, IMDId *mdid);
};

}  // namespace gpdxl
