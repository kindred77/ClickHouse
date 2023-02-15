#pragma once


#include <Interpreters/orcaopt/parser_common.h>

#include <map>
#include <optional>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

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

    char * get_database_name(Oid dbid);

    char get_rel_relkind(Oid relid);
};

}
