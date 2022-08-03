#include <Interpreters/orcaopt/pgopt/RelationProvider.h>

namespace DB
{

RelationProvider::RelationProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
		: context(std::move(context_)),
		  mp(std::move(mp_))
{
	oid_storageid_map.insert(std::pair<Oid, StoragePtr>(Oid(1), DatabaseCatalog::instance().getTable(StorageID("system", "one"), context)));
}

StoragePtr
RelationProvider::getStorageByOID(Oid oid)
{
	auto it = oid_storageid_map.find(oid);
	if (it == oid_storageid_map.end())
	    return {};
	return it->second;
}

std::optional<std::tuple<Oid, StoragePtr, char> >
RelationProvider::getPairByDBAndTableName(const String & database_name, const String & table_name)
{
	auto it = oid_storageid_map.find(oid);
	if (it == oid_storageid_map.end())
	    return nullptr;
	return {it->first, it->second, 'r'};
}

}