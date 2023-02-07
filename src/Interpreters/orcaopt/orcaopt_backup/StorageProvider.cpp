
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

std::pair<OID, StoragePtr>*
StorageProvider::getPairByDBAndTableName(const ASTPtr database_and_table_name)
{
	auto it = oid_storageid_map.find(oid);
	if (it == oid_storageid_map.end())
	    return {};
	return it->second;
}

}


