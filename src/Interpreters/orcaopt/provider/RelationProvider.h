#pragma once


#include <common/parser_common.hpp>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace rocksdb
{
    class DB;
}

namespace DB
{

class TypeProvider;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

using String = std::string;

typedef void (*RangeVarGetRelidCallback)(const duckdb_libpgquery::PGRangeVar * relation, duckdb_libpgquery::PGOid relId, duckdb_libpgquery::PGOid oldRelId, void * callback_arg);

typedef int LOCKMODE;

class RelationProvider
{
private:
    //using ClassMap = std::map<duckdb_libpgquery::PGOid, PGClassPtr>;
	//ClassMap oid_class_map;

	using RelationMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGRelationPtr>;
	RelationMap oid_relation_map;

    using DatabaseMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGDatabasePtr>;
	DatabaseMap oid_database_map;

    TypeProviderPtr type_provider;

	const ContextPtr context;
	//gpos::CMemoryPool *mp;
    static int RELATION_OID_ID;
    static int DATABASE_OID_ID;
    const String max_database_oid_rocksdb_key = "max_database_oid";
    const String max_table_oid_rocksdb_key = "max_table_oid";

    using RocksDBPtr = std::shared_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;

    String rocksdb_dir;
    const String relative_data_path_ = "relation_meta_db";

    void initDb();

    void initAttrs(duckdb_libpgquery::PGRelationPtr & relation);
public:
	//explicit RelationProvider(gpos::CMemoryPool *mp_, ContextPtr context);
    explicit RelationProvider(ContextPtr& context_);
	//StoragePtr getStorageByOID(duckdb_libpgquery::PGOid oid) const;

	//std::optional<std::tuple<duckdb_libpgquery::PGOid, StoragePtr, char> >
	//getPairByDBAndTableName(const String & database_name, const String & table_name) const;

    duckdb_libpgquery::PGClassPtr getClassByRelOid(duckdb_libpgquery::PGOid oid) const;

    bool has_subclass(duckdb_libpgquery::PGOid relationId);

    const std::string get_database_name(duckdb_libpgquery::PGOid dbid) const;

    char get_rel_relkind(duckdb_libpgquery::PGOid relid) const;

	const std::string get_attname(duckdb_libpgquery::PGOid relid, duckdb_libpgquery::PGAttrNumber attnum) const;

    const std::string get_rel_name(duckdb_libpgquery::PGOid relid);

    duckdb_libpgquery::PGAttrPtr get_att_by_reloid_attnum(duckdb_libpgquery::PGOid relid, duckdb_libpgquery::PGAttrNumber attnum) const;

	std::string get_rte_attribute_name(duckdb_libpgquery::PGRangeTblEntry * rte, duckdb_libpgquery::PGAttrNumber attnum);

    duckdb_libpgquery::PGOid RangeVarGetRelidExtended(
        const duckdb_libpgquery::PGRangeVar * relation, LOCKMODE lockmode, bool missing_ok,
		bool nowait, RangeVarGetRelidCallback callback, void * callback_arg);

    duckdb_libpgquery::PGRelationPtr relation_open(duckdb_libpgquery::PGOid relationId, LOCKMODE lockmode);

    void relation_close(duckdb_libpgquery::PGRelationPtr relation, LOCKMODE lockmode);

    duckdb_libpgquery::PGRelationPtr heap_open(duckdb_libpgquery::PGOid relationId, LOCKMODE lockmode);

    duckdb_libpgquery::PGRelationPtr try_heap_open(duckdb_libpgquery::PGOid relationId, LOCKMODE lockmode, bool noWait);

    bool IsSystemRelation(duckdb_libpgquery::PGRelationPtr relation);

    duckdb_libpgquery::PGOid get_relname_relid(const char * relname, duckdb_libpgquery::PGOid relnamespace);

    duckdb_libpgquery::PGOid LookupNamespaceNoError(const char * nspname);

    //PGAttrPtr SystemAttributeByName(const char * attname, bool relhasoids);

    //PGPolicyPtr PGPolicyFetch(duckdb_libpgquery::PGOid tbloid);

    bool PGPolicyIsReplicated(const duckdb_libpgquery::PGPolicyPtr policy);

	// bool AttrExistsInRel(duckdb_libpgquery::PGOid rel_oid, int attr_no);

    duckdb_libpgquery::PGAttrNumber get_attnum(duckdb_libpgquery::PGOid relid, const char * attname);

    duckdb_libpgquery::PGOid get_atttype(duckdb_libpgquery::PGOid relid, duckdb_libpgquery::PGAttrNumber attnum);

    duckdb_libpgquery::PGOid LookupExplicitNamespace(const char * nspname, bool missing_ok);

    std::string get_namespace_name(duckdb_libpgquery::PGOid nspid);

    duckdb_libpgquery::PGOid get_rel_type_id(duckdb_libpgquery::PGOid relid);
};

}
