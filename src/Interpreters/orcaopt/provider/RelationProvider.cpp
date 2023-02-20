#include <Interpreters/orcaopt/provider/RelationProvider.h>

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

std::string RelationProvider::get_database_name(Oid dbid)
{
    HeapTuple dbtuple;
    std::string result = "";

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

std::string RelationProvider::get_attname(Oid relid, PGAttrNumber attnum) const
{
    std::string result = "";

    return result;
};

std::string RelationProvider::get_rte_attribute_name(PGRangeTblEntry * rte, PGAttrNumber attnum)
{
    const char * name;

    if (attnum == InvalidAttrNumber)
        return "*";

    /*
	 * If there is a user-written column alias, use it.
	 */
    if (rte->alias && attnum > 0 && attnum <= list_length(rte->alias->colnames))
        return strVal(list_nth(rte->alias->colnames, attnum - 1));

    /*
	 * CDB: Pseudo columns have negative attribute numbers below the
	 * lowest system attribute number.
	 */
	//TODO kindred
    // if (attnum <= FirstLowInvalidHeapAttributeNumber)
    // {
    //     CdbRelColumnInfo * rci = cdb_rte_find_pseudo_column(rte, attnum);

    //     if (!rci)
    //         goto bogus;
    //     return rci->colname;
    // }

    /*
	 * If the RTE is a relation, go to the system catalogs not the
	 * eref->colnames list.  This is a little slower but it will give the
	 * right answer if the column has been renamed since the eref list was
	 * built (which can easily happen for rules).
	 */
    if (rte->rtekind == duckdb_libpgquery::PG_RTE_RELATION)
        return get_attname(rte->relid, attnum);

    /*
	 * Otherwise use the column name from eref.  There should always be one.
	 */
    if (rte->eref != NULL && attnum > 0 && attnum <= list_length(rte->eref->colnames))
        return strVal(list_nth(rte->eref->colnames, attnum - 1));

    /* CDB: Get name of sysattr even if relid is no good (e.g. SubqueryScan) */
	//TODO kindred
    // if (attnum < 0 && attnum > FirstLowInvalidHeapAttributeNumber)
    // {
    //     Form_pg_attribute att_tup = SystemAttributeDefinition(attnum, true);

    //     return NameStr(att_tup->attname);
    // }

bogus:
    /* else caller gave us a bogus attnum */
    name = (rte->eref && rte->eref->aliasname) ? rte->eref->aliasname : "*BOGUS*";
    ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR), errmsg_internal("invalid attnum %d for rangetable entry %s", attnum, name)));
    return "*BOGUS*";
};

}
