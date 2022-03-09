/*
 * StorageProvider.cpp
 *
 *  Created on: 2022Äê3ÔÂ2ÈÕ
 *      Author: tangye
 */

#include <Interpreters/orcaopt/StorageProvider.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

StorageProvider::StorageProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
		: context(std::move(context_)),
		  mp(std::move(mp_))
{
	oid_storageid_map.insert(std::pair<OID, StoragePtr>(OID(1), DatabaseCatalog::instance().getTable(StorageID("system", "one"), context)));
}

StoragePtr
StorageProvider::getStorageByOID(OID oid)
{
	auto it = oid_storageid_map.find(oid);
	if (it == oid_storageid_map.end())
	    return {};
	return it->second;
}

}


