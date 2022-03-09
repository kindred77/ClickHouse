#pragma once

#include "naucrates/dxl/gpdb_types.h"

#include <Storages/IStorage.h>

#include <map>

namespace DB
{
class StorageProvider;
using StorageProviderPtr = std::shared_ptr<const StorageProvider>;

class StorageProvider
{
private:
	using Map = std::map<OID, StoragePtr>;

	Map oid_storageid_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;
public:
	explicit StorageProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	StoragePtr getStorageByOID(OID oid);
};

}
