#pragma once


#include <Storages/IStorage.h>

#include <Interpreters/orcaopt/pgopt/parser_common.h>

#include <map>

namespace DB
{

class RelationProvider
{
private:
	using Map = std::map<Oid, StoragePtr>;

	Map oid_storageid_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;
public:
	explicit RelationProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	StoragePtr getStorageByOID(Oid oid) const;

	std::optional<std::tuple<Oid, StoragePtr, char> >
	getPairByDBAndTableName(const String & database_name, const String & table_name) const;
};

}