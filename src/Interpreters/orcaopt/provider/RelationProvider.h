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
	static RelationMap oid_relation_map;

    using DatabaseMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGDatabasePtr>;
	static DatabaseMap oid_database_map;

    //TypeProviderPtr type_provider;

	static ContextPtr context;
    static bool is_init;
	//gpos::CMemoryPool *mp;
    static int RELATION_OID_ID;
    static int DATABASE_OID_ID;
    static const String max_database_oid_rocksdb_key;
    static const String max_table_oid_rocksdb_key;

    using RocksDBPtr = std::shared_ptr<rocksdb::DB>;
    static RocksDBPtr rocksdb_ptr;

    static String rocksdb_dir;
    static const String relative_data_path_;

    static void initDb();

    static void initAttrs(duckdb_libpgquery::PGRelationPtr & relation);

public:
    static void mockTestData();
	//explicit RelationProvider(gpos::CMemoryPool *mp_, ContextPtr context);
    static void Init(ContextPtr& context_);
	//StoragePtr getStorageByOID(duckdb_libpgquery::PGOid oid) const;

	//std::optional<std::tuple<duckdb_libpgquery::PGOid, StoragePtr, char> >
	//getPairByDBAndTableName(const String & database_name, const String & table_name) const;

    static duckdb_libpgquery::PGClassPtr getClassByRelOid(duckdb_libpgquery::PGOid oid);

    static bool has_subclass(duckdb_libpgquery::PGOid relationId);

    static const std::string get_database_name(duckdb_libpgquery::PGOid dbid);

    static char get_rel_relkind(duckdb_libpgquery::PGOid relid);

	static const std::string get_attname(duckdb_libpgquery::PGOid relid, duckdb_libpgquery::PGAttrNumber attnum);

    static const std::string get_rel_name(duckdb_libpgquery::PGOid relid);

    static duckdb_libpgquery::PGAttrPtr get_att_by_reloid_attnum(duckdb_libpgquery::PGOid relid, duckdb_libpgquery::PGAttrNumber attnum);

	static std::string get_rte_attribute_name(duckdb_libpgquery::PGRangeTblEntry * rte, duckdb_libpgquery::PGAttrNumber attnum);

    static duckdb_libpgquery::PGOid RangeVarGetRelidExtended(
        const duckdb_libpgquery::PGRangeVar * relation, LOCKMODE lockmode, bool missing_ok,
		bool nowait, RangeVarGetRelidCallback callback, void * callback_arg);

    static duckdb_libpgquery::PGRelationPtr relation_open(duckdb_libpgquery::PGOid relationId, LOCKMODE lockmode);

    static void relation_close(duckdb_libpgquery::PGRelationPtr relation, LOCKMODE lockmode);

    static duckdb_libpgquery::PGRelationPtr heap_open(duckdb_libpgquery::PGOid relationId, LOCKMODE lockmode);

    static duckdb_libpgquery::PGRelationPtr try_heap_open(duckdb_libpgquery::PGOid relationId, LOCKMODE lockmode, bool noWait);

    static bool IsSystemRelation(duckdb_libpgquery::PGRelationPtr relation);

    static duckdb_libpgquery::PGOid get_relname_relid(const char * relname, duckdb_libpgquery::PGOid relnamespace);

    static duckdb_libpgquery::PGOid LookupNamespaceNoError(const char * nspname);

    //PGAttrPtr SystemAttributeByName(const char * attname, bool relhasoids);

    //PGPolicyPtr PGPolicyFetch(duckdb_libpgquery::PGOid tbloid);

    static bool PGPolicyIsReplicated(const duckdb_libpgquery::PGPolicyPtr policy);

	// bool AttrExistsInRel(duckdb_libpgquery::PGOid rel_oid, int attr_no);

    static duckdb_libpgquery::PGAttrNumber get_attnum(duckdb_libpgquery::PGOid relid, const char * attname);

    static duckdb_libpgquery::PGOid get_atttype(duckdb_libpgquery::PGOid relid, duckdb_libpgquery::PGAttrNumber attnum);

    static duckdb_libpgquery::PGOid LookupExplicitNamespace(const char * nspname, bool missing_ok);

    static std::string get_namespace_name(duckdb_libpgquery::PGOid nspid);

    static duckdb_libpgquery::PGOid get_rel_type_id(duckdb_libpgquery::PGOid relid);
};

}
