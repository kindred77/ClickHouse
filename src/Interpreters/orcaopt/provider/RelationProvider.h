#pragma once


#include <Interpreters/orcaopt/parser_common.h>

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

typedef void (*RangeVarGetRelidCallback)(const duckdb_libpgquery::PGRangeVar * relation, Oid relId, Oid oldRelId, void * callback_arg);

typedef int LOCKMODE;

class RelationProvider
{
private:
    //using ClassMap = std::map<Oid, PGClassPtr>;
	//ClassMap oid_class_map;

	using RelationMap = std::map<Oid, PGRelationPtr>;
	RelationMap oid_relation_map;

    using DatabaseMap = std::map<Oid, PGDatabasePtr>;
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

    void initAttrs(PGRelationPtr & relation);
public:
	//explicit RelationProvider(gpos::CMemoryPool *mp_, ContextPtr context);
    explicit RelationProvider(ContextPtr& context_);
	//StoragePtr getStorageByOID(Oid oid) const;

	//std::optional<std::tuple<Oid, StoragePtr, char> >
	//getPairByDBAndTableName(const String & database_name, const String & table_name) const;

    PGClassPtr getClassByRelOid(Oid oid) const;

    bool has_subclass(Oid relationId);

    const std::string get_database_name(Oid dbid) const;

    char get_rel_relkind(Oid relid) const;

	const std::string get_attname(Oid relid, PGAttrNumber attnum) const;

    const std::string get_rel_name(Oid relid);

    PGAttrPtr get_att_by_reloid_attnum(Oid relid, PGAttrNumber attnum) const;

	std::string get_rte_attribute_name(duckdb_libpgquery::PGRangeTblEntry * rte, PGAttrNumber attnum);

    Oid RangeVarGetRelidExtended(
        const duckdb_libpgquery::PGRangeVar * relation, LOCKMODE lockmode, bool missing_ok,
		bool nowait, RangeVarGetRelidCallback callback, void * callback_arg);

    PGRelationPtr relation_open(Oid relationId, LOCKMODE lockmode);

    void relation_close(PGRelationPtr relation, LOCKMODE lockmode);

    PGRelationPtr heap_open(Oid relationId, LOCKMODE lockmode);

    PGRelationPtr try_heap_open(Oid relationId, LOCKMODE lockmode, bool noWait);

    bool IsSystemRelation(PGRelationPtr relation);

    Oid get_relname_relid(const char * relname, Oid relnamespace);

    Oid LookupNamespaceNoError(const char * nspname);

    //PGAttrPtr SystemAttributeByName(const char * attname, bool relhasoids);

    //PGPolicyPtr PGPolicyFetch(Oid tbloid);

    bool PGPolicyIsReplicated(const PGPolicyPtr policy);

	// bool AttrExistsInRel(Oid rel_oid, int attr_no);

    PGAttrNumber get_attnum(Oid relid, const char * attname);

    Oid get_atttype(Oid relid, PGAttrNumber attnum);

    Oid LookupExplicitNamespace(const char * nspname, bool missing_ok);

    std::string get_namespace_name(Oid nspid);
};

}
