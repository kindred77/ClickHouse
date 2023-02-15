#include <Interpreters/orcaopt/RelationProvider.h>

#include <Storages/IStorage.h>

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

char * RelationProvider::get_database_name(Oid dbid)
{
    HeapTuple dbtuple;
    char * result;

    dbtuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
    if (HeapTupleIsValid(dbtuple))
    {
        result = pstrdup(NameStr(((Form_pg_database)GETSTRUCT(dbtuple))->datname));
        ReleaseSysCache(dbtuple);
    }
    else
        result = NULL;

    return result;
};

char RelationProvider::get_rel_relkind(Oid relid)
{
    HeapTuple tp;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp))
    {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        char result;

        result = reltup->relkind;
        ReleaseSysCache(tp);
        return result;
    }
    else
        return '\0';
};

}
