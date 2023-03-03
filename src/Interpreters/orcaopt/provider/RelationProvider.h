#pragma once


#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>
#include <gpos/memory/CMemoryPool.h>

#include <map>
#include <optional>

namespace DB
{
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

typedef void (*RangeVarGetRelidCallback)(const duckdb_libpgquery::PGRangeVar * relation, Oid relId, Oid oldRelId, void * callback_arg);

typedef int LOCKMODE;

class RelationProvider
{
private:
	using Map = std::map<Oid, StoragePtr>;

	Map oid_storageid_map;
	//ContextPtr context;
	//gpos::CMemoryPool *mp;
public:
	//explicit RelationProvider(gpos::CMemoryPool *mp_, ContextPtr context);
    explicit RelationProvider();
	StoragePtr getStorageByOID(Oid oid) const;

	//std::optional<std::tuple<Oid, StoragePtr, char> >
	//getPairByDBAndTableName(const String & database_name, const String & table_name) const;

    std::string get_database_name(Oid dbid) const;

    char get_rel_relkind(Oid relid) const;

	std::string get_attname(Oid relid, PGAttrNumber attnum) const;

    const char * get_rel_name(Oid relid);

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

    PGRelationPtr parserOpenTable(PGParseState * pstate, const duckdb_libpgquery::PGRangeVar * relation, int lockmode, bool * lockUpgraded);

    Oid get_relname_relid(const char * relname, Oid relnamespace);

    Oid LookupNamespaceNoError(const char * nspname);

    PGAttrPtr SystemAttributeByName(const char * attname, bool relhasoids);

    PGPolicyPtr PGPolicyFetch(Oid tbloid);

    bool PGPolicyIsReplicated(const PGPolicyPtr policy);

	bool AttrExistsInRel(Oid rel_oid, int attr_no);

    PGAttrNumber get_attnum(Oid relid, const char * attname);

    Oid get_atttype(Oid relid, PGAttrNumber attnum);

    Oid LookupExplicitNamespace(const char * nspname, bool missing_ok);

    std::string get_namespace_name(Oid nspid);
};

}
