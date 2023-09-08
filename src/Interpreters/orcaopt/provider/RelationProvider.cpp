#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>

#include <filesystem>

using namespace duckdb_libpgquery;

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

int RelationProvider::RELATION_OID_ID = 0;
int RelationProvider::DATABASE_OID_ID = 0;

const String RelationProvider::max_database_oid_rocksdb_key = "max_database_oid";
const String RelationProvider::max_table_oid_rocksdb_key = "max_table_oid";
const String RelationProvider::relative_data_path_ = "relation_meta_db";
String RelationProvider::rocksdb_dir = "";
RelationProvider::RocksDBPtr RelationProvider::rocksdb_ptr = nullptr;
RelationProvider::RelationMap RelationProvider::oid_relation_map = {};
RelationProvider::DatabaseMap RelationProvider::oid_database_map = {};

ContextPtr RelationProvider::context = nullptr;
bool RelationProvider::is_init = false;

void RelationProvider::initDb()
{
    if (is_init) return;
    rocksdb_dir = context->getPath() + relative_data_path_;

    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kZSTD;
    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);

    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " +  rocksdb_dir + ": " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
    rocksdb_ptr = std::shared_ptr<rocksdb::DB>(db);

    // get max database oid
    String max_oid;
    status = rocksdb_ptr->Get(rocksdb::ReadOptions(), rocksdb::Slice(max_database_oid_rocksdb_key), &max_oid);
    if (status.ok())
    {
        DATABASE_OID_ID = PGOid(std::stoi(max_oid));
    }
    // get max table oid
    status = rocksdb_ptr->Get(rocksdb::ReadOptions(), rocksdb::Slice(max_table_oid_rocksdb_key), &max_oid);
    if (status.ok())
    {
        RELATION_OID_ID = PGOid(std::stoi(max_oid));
    }
};

void RelationProvider::initAttrs(PGRelationPtr & relation)
{
    if (is_init) return;
    PGAttrNumber attr_num = 1;
    IStorage* storage = relation->storage_ptr.get();
    auto all_cols = storage->getInMemoryMetadataPtr()->getColumns().getAll();
    auto virtual_cols = storage->getVirtuals();
    all_cols.merge(virtual_cols);
    for (auto name_and_type : all_cols)
    {
        auto type_ptr = TypeProvider::get_type_by_typename_namespaceoid(name_and_type.type->getName());
        TypeProvider::PGTupleDescInitEntry(relation->rd_att, attr_num,
            name_and_type.name, /*oidtypeid*/ type_ptr->oid, /*typmod*/ type_ptr->typtypmod, /*attdim*/ 0);
        attr_num++;
    }
};

void RelationProvider::mockTestData()
{
    auto db_ptr = std::make_shared<PGDatabase>(PGDatabase{static_cast<PGOid>(999999), "test"});
    oid_database_map.insert({db_ptr->oid, db_ptr});

    auto table_oid = 9999;
    auto table_name = "test";
    std::map<String, String> cols = {{"String", "col1"}, {"Int64", "col2"}, {"DateTime", "col3"}, {"Int8", "col4"}, {"Date", "col5"}};
    auto tab_class = std::make_shared<Form_pg_class>(Form_pg_class{
        /*oid*/ PGOid(table_oid),
        /*relname*/ table_name,
        /*relnamespace*/ db_ptr->oid,
        /*reltype*/ InvalidOid,
        /*reloftype*/ InvalidOid,
        /*relowner*/ InvalidOid,
        /*relam*/ InvalidOid,
        /*relfilenode*/ InvalidOid,
        /*reltablespace*/ InvalidOid,
        /*relpages*/ 0,
        /*reltuples*/ 0,
        /*relallvisible*/ 0,
        /*reltoastrelid*/ InvalidOid,
        /*relhasindex*/ false,
        /*relisshared*/ false,
        /*relpersistence*/ 'p',
        /*relkind*/ 'r',
        /*relstorage*/ 'a',
        /*relnatts*/ static_cast<int16>(cols.size()),
        /*relchecks*/ 0,
        /*relhasoids*/ false,
        /*relhaspkey*/ false,
        /*relhasrules*/ false,
        /*relhastriggers*/ false,
        /*relhassubclass*/ false,
        /*relispopulated*/ false,
        /*relreplident*/ 'd',
        /*relfrozenxid*/ 0,
        /*relminmxid*/ 0
    });

    // oid of class and oid of relation is the same
    auto tab_rel = std::make_shared<PGRelation>(PGRelation{
        /*oid*/ .oid = tab_class->oid,
        // /*rd_node*/ {},
        // /*rd_refcnt*/ 0,
        // /*rd_backend*/ 0,
        // /*rd_islocaltemp*/ false,
        // /*rd_isnailed*/ false,
        // /*rd_isvalid*/ false,
        // /*rd_indexvalid*/ false,
        // /*rd_createSubid*/ 0,
        // /*rd_newRelfilenodeSubid*/ 0,
        /*rd_rel*/ .rd_rel = tab_class,
        /*rd_att*/ .rd_att = PGCreateTemplateTupleDesc(tab_class->relnatts, tab_class->relhasoids),
         /*rd_id*/ tab_class->oid,
        // /*rd_lockInfo*/ {},
        // /*rd_cdbpolicy*/ {},
        // /*rd_cdbDefaultStatsWarningIssued*/ false,
        // /*rd_indexlist*/ NULL,
        // /*rd_oidindex*/ InvalidOid,
        // /*rd_replidindex*/ InvalidOid,
        // /*rd_indexattr*/ NULL,
        // /*rd_keyattr*/ NULL,
        // /*rd_idattr*/ NULL,
        // /*rd_options*/ NULL,
        // /*rd_opfamily*/ NULL,
        // /*rd_opcintype*/ NULL,
        // /*rd_indoption*/ NULL,
        // /*rd_indexprs*/ NULL,
        // /*rd_indpred*/ NULL,
        // /*rd_exclops*/ NULL,
        // /*rd_exclprocs*/ NULL,
        // /*rd_exclstrats*/ NULL,
        // /*rd_amcache*/ NULL,
        // /*rd_indcollation*/ NULL,
        // /*rd_toastoid*/ InvalidOid
    });

    PGAttrNumber attr_num = 1;
    for (auto type_and_name : cols)
    {
        auto type_ptr = TypeProvider::get_type_by_typename_namespaceoid(type_and_name.first);
        if (!type_ptr)
        {
            elog(ERROR, "Unknow type: %s.", type_and_name.first.c_str());
            return;
        }
        TypeProvider::PGTupleDescInitEntry(tab_rel->rd_att, attr_num,
            type_and_name.second, /*oidtypeid*/ type_ptr->oid, /*typmod*/ type_ptr->typtypmod, /*attdim*/ 0);
        attr_num++;
    }

    oid_relation_map.insert({tab_rel->oid, tab_rel});
};

void RelationProvider::Init(ContextPtr& context_)
{
    if (is_init) return;
    context = context_;
    initDb();
    
    for (const auto & pair : DatabaseCatalog::instance().getDatabases())
    {
        const auto & database_name = pair.first;

        String database_oid;
        auto status = rocksdb_ptr->Get(rocksdb::ReadOptions(), database_name, &database_oid);
        if (!status.ok())
        {
            rocksdb::WriteBatch batch;
            batch.Put(database_name, std::to_string(++DATABASE_OID_ID));
            database_oid = std::to_string(DATABASE_OID_ID);
            batch.Put(max_database_oid_rocksdb_key, database_oid);
            status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
            if (!status.ok())
                throw Exception("Can not load database "+database_name+" to RocksDB, write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
        }

        auto db_ptr = std::make_shared<PGDatabase>(PGDatabase{static_cast<PGOid>(std::stoi(database_oid)), database_name});
        oid_database_map.insert({db_ptr->oid, db_ptr});

        const auto & database = pair.second;
        for (auto tables_it = database->getTablesIterator(context);
            tables_it->isValid();
            tables_it->next())
        {
            auto table_name = tables_it->name();
            String table_oid;
            status = rocksdb_ptr->Get(rocksdb::ReadOptions(), table_name, &table_oid);
            if (!status.ok())
            {
                rocksdb::WriteBatch batch;
                batch.Put(table_name, std::to_string(++RELATION_OID_ID));
                table_oid = std::to_string(RELATION_OID_ID);
                batch.Put(max_table_oid_rocksdb_key, table_oid);
                status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
                if (!status.ok())
                    throw Exception(
                        "Can not load table name " + table_name + " to RocksDB, write error: " + status.ToString(),
                        ErrorCodes::ROCKSDB_ERROR);
            }
            
            auto tab_class = std::make_shared<Form_pg_class>(Form_pg_class{
                /*oid*/ PGOid(std::stoi(table_oid)),
                /*relname*/ table_name,
                /*relnamespace*/ db_ptr->oid,
                /*reltype*/ InvalidOid,
                /*reloftype*/ InvalidOid,
                /*relowner*/ InvalidOid,
                /*relam*/ InvalidOid,
                /*relfilenode*/ InvalidOid,
                /*reltablespace*/ InvalidOid,
                /*relpages*/ 0,
                /*reltuples*/ 0,
                /*relallvisible*/ 0,
                /*reltoastrelid*/ InvalidOid,
                /*relhasindex*/ false,
                /*relisshared*/ false,
                /*relpersistence*/ 'p',
                /*relkind*/ 'r',
                /*relstorage*/ 'a',
                /*relnatts*/ static_cast<int16>(tables_it->table()->getInMemoryMetadataPtr()->getColumns().getAllPhysical().size()),
                /*relchecks*/ 0,
                /*relhasoids*/ false,
                /*relhaspkey*/ false,
                /*relhasrules*/ false,
                /*relhastriggers*/ false,
                /*relhassubclass*/ false,
                /*relispopulated*/ false,
                /*relreplident*/ 'd',
                /*relfrozenxid*/ 0,
                /*relminmxid*/ 0
            });

            // oid of class and oid of relation is the same
            auto tab_rel = std::make_shared<PGRelation>(PGRelation{
                /*oid*/ .oid = tab_class->oid,
                // /*rd_node*/ {},
                // /*rd_refcnt*/ 0,
                // /*rd_backend*/ 0,
                // /*rd_islocaltemp*/ false,
                // /*rd_isnailed*/ false,
                // /*rd_isvalid*/ false,
                // /*rd_indexvalid*/ false,
                // /*rd_createSubid*/ 0,
                // /*rd_newRelfilenodeSubid*/ 0,
                /*rd_rel*/ .rd_rel = tab_class,
                /*rd_att*/ .rd_att = PGCreateTemplateTupleDesc(tab_class->relnatts, tab_class->relhasoids),
                /*rd_id*/ tab_class->oid,
                // /*rd_lockInfo*/ {},
                // /*rd_cdbpolicy*/ {},
                // /*rd_cdbDefaultStatsWarningIssued*/ false,
                // /*rd_indexlist*/ NULL,
                // /*rd_oidindex*/ InvalidOid,
                // /*rd_replidindex*/ InvalidOid,
                // /*rd_indexattr*/ NULL,
                // /*rd_keyattr*/ NULL,
                // /*rd_idattr*/ NULL,
                // /*rd_options*/ NULL,
                // /*rd_opfamily*/ NULL,
                // /*rd_opcintype*/ NULL,
                // /*rd_indoption*/ NULL,
                // /*rd_indexprs*/ NULL,
                // /*rd_indpred*/ NULL,
                // /*rd_exclops*/ NULL,
                // /*rd_exclprocs*/ NULL,
                // /*rd_exclstrats*/ NULL,
                // /*rd_amcache*/ NULL,
                // /*rd_indcollation*/ NULL,
                // /*rd_toastoid*/ InvalidOid
            });

            initAttrs(tab_rel);

            oid_relation_map.insert({tab_rel->oid, tab_rel});
        }
    }

    SCOPE_EXIT({rocksdb_ptr->Close();});
    is_init = true;
};

// StoragePtr
// RelationProvider::getStorageByOID(PGOid oid) const
// {
// 	auto it = oid_storageid_map.find(oid);
// 	if (it == oid_storageid_map.end())
// 	    return {};
// 	return it->second;
// };

// std::optional<std::tuple<PGOid, StoragePtr, char> >
// RelationProvider::getPairByDBAndTableName(const String & database_name, const String & table_name) const
// {
// 	auto it = oid_storageid_map.find(oid);
// 	if (it == oid_storageid_map.end())
// 	    return nullptr;
// 	return {it->first, it->second, 'r'};
// }

PGClassPtr RelationProvider::getClassByRelOid(PGOid oid)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

	auto it = oid_relation_map.find(oid);
	if (it == oid_relation_map.end())
	    return nullptr;
	return it->second->rd_rel;
};

bool RelationProvider::has_subclass(PGOid relationId)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

	PGClassPtr tuple = getClassByRelOid(relationId);
	if (tuple == nullptr)
	{
		elog(ERROR, "cache lookup failed for relation %u", relationId);
		return false;
	}

	return tuple->relhassubclass;
};

const std::string RelationProvider::get_database_name(PGOid dbid)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    auto it = oid_database_map.find(dbid);
	if (it == oid_database_map.end())
	    return "";
	return it->second->name;
};

char RelationProvider::get_rel_relkind(PGOid relid)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    auto it = oid_relation_map.find(relid);
	if (it == oid_relation_map.end())
	    return '\0';
	return it->second->rd_rel->relkind;
};

const std::string RelationProvider::get_attname(PGOid relid, PGAttrNumber attnum)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    auto it = oid_relation_map.find(relid);
	if (it == oid_relation_map.end())
	    return "";
	for (const auto& att : it->second->rd_att->attrs)
    {
        if (att->attnum == attnum)
        {
            return att->attname;
        }
    }

    return "";
};

const std::string RelationProvider::get_rel_name(PGOid relid)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    auto it = oid_relation_map.find(relid);
	if (it == oid_relation_map.end())
	    return "";
	return it->second->rd_rel->relname;
};

PGAttrPtr RelationProvider::get_att_by_reloid_attnum(PGOid relid, PGAttrNumber attnum)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    auto it = oid_relation_map.find(relid);
	if (it == oid_relation_map.end())
	    return nullptr;
	return it->second->rd_att->attrs[attnum - 1];
};

std::string RelationProvider::get_rte_attribute_name(PGRangeTblEntry * rte, PGAttrNumber attnum)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

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
    ereport(WARNING, (errcode(PG_ERRCODE_INTERNAL_ERROR), errmsg_internal("invalid attnum %d for rangetable entry %s", attnum, name)));
    return "*BOGUS*";
};

PGOid RelationProvider::RangeVarGetRelidExtended(
        const PGRangeVar * relation, LOCKMODE lockmode, bool missing_ok,
		bool nowait, RangeVarGetRelidCallback callback, void * callback_arg)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    // uint64 inval_count;
    // Oid relId;
    // Oid oldRelId = InvalidOid;
    // bool retry = false;

    // /*
	//  * We check the catalog name and then ignore it.
	//  */
    // if (relation->catalogname)
    // {
    //     if (strcmp(relation->catalogname, get_database_name(MyDatabaseId)) != 0)
    //         ereport(
    //             ERROR,
    //             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //              errmsg(
    //                  "cross-database references are not implemented: \"%s.%s.%s\"",
    //                  relation->catalogname,
    //                  relation->schemaname,
    //                  relation->relname)));
    // }

    // /*
	//  * DDL operations can change the results of a name lookup.  Since all such
	//  * operations will generate invalidation messages, we keep track of
	//  * whether any such messages show up while we're performing the operation,
	//  * and retry until either (1) no more invalidation messages show up or (2)
	//  * the answer doesn't change.
	//  *
	//  * But if lockmode = NoLock, then we assume that either the caller is OK
	//  * with the answer changing under them, or that they already hold some
	//  * appropriate lock, and therefore return the first answer we get without
	//  * checking for invalidation messages.  Also, if the requested lock is
	//  * already held, no LockRelationOid will not AcceptInvalidationMessages,
	//  * so we may fail to notice a change.  We could protect against that case
	//  * by calling AcceptInvalidationMessages() before beginning this loop, but
	//  * that would add a significant amount overhead, so for now we don't.
	//  */
    // for (;;)
    // {
    //     /*
	// 	 * Remember this value, so that, after looking up the relation name
	// 	 * and locking its OID, we can check whether any invalidation messages
	// 	 * have been processed that might require a do-over.
	// 	 */
    //     inval_count = SharedInvalidMessageCounter;

    //     /*
	// 	 * Some non-default relpersistence value may have been specified.  The
	// 	 * parser never generates such a RangeVar in simple DML, but it can
	// 	 * happen in contexts such as "CREATE TEMP TABLE foo (f1 int PRIMARY
	// 	 * KEY)".  Such a command will generate an added CREATE INDEX
	// 	 * operation, which must be careful to find the temp table, even when
	// 	 * pg_temp is not first in the search path.
	// 	 */
    //     if (relation->relpersistence == RELPERSISTENCE_TEMP)
    //     {
    //         if (!OidIsValid(myTempNamespace))
    //             relId = InvalidOid; /* this probably can't happen? */
    //         else
    //         {
    //             if (relation->schemaname)
    //             {
    //                 Oid namespaceId;

    //                 namespaceId = LookupExplicitNamespace(relation->schemaname, missing_ok);

    //                 /*
	// 				 * For missing_ok, allow a non-existant schema name to
	// 				 * return InvalidOid.
	// 				 */
    //                 if (namespaceId != myTempNamespace)
    //                     ereport(
    //                         ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("temporary tables cannot specify a schema name")));
    //             }

    //             relId = get_relname_relid(relation->relname, myTempNamespace);
    //         }
    //     }
    //     else if (relation->schemaname)
    //     {
    //         Oid namespaceId;

    //         /* use exact schema given */
    //         namespaceId = LookupExplicitNamespace(relation->schemaname, missing_ok);
    //         if (missing_ok && !OidIsValid(namespaceId))
    //             relId = InvalidOid;
    //         else
    //             relId = get_relname_relid(relation->relname, namespaceId);
    //     }
    //     else
    //     {
    //         /* search the namespace path */
    //         relId = RelnameGetRelid(relation->relname);
    //     }

    //     /*
	// 	 * Invoke caller-supplied callback, if any.
	// 	 *
	// 	 * This callback is a good place to check permissions: we haven't
	// 	 * taken the table lock yet (and it's really best to check permissions
	// 	 * before locking anything!), but we've gotten far enough to know what
	// 	 * OID we think we should lock.  Of course, concurrent DDL might
	// 	 * change things while we're waiting for the lock, but in that case
	// 	 * the callback will be invoked again for the new OID.
	// 	 */
    //     if (callback)
    //         callback(relation, relId, oldRelId, callback_arg);

    //     /*
	// 	 * If no lock requested, we assume the caller knows what they're
	// 	 * doing.  They should have already acquired a heavyweight lock on
	// 	 * this relation earlier in the processing of this same statement, so
	// 	 * it wouldn't be appropriate to AcceptInvalidationMessages() here, as
	// 	 * that might pull the rug out from under them.
	// 	 */
    //     if (lockmode == NoLock)
    //         break;

    //     /*
	// 	 * If, upon retry, we get back the same OID we did last time, then the
	// 	 * invalidation messages we processed did not change the final answer.
	// 	 * So we're done.
	// 	 *
	// 	 * If we got a different OID, we've locked the relation that used to
	// 	 * have this name rather than the one that does now.  So release the
	// 	 * lock.
	// 	 */
    //     if (retry)
    //     {
    //         if (relId == oldRelId)
    //             break;
    //         if (OidIsValid(oldRelId))
    //             UnlockRelationOid(oldRelId, lockmode);
    //     }

    //     /*
	// 	 * Lock relation.  This will also accept any pending invalidation
	// 	 * messages.  If we got back InvalidOid, indicating not found, then
	// 	 * there's nothing to lock, but we accept invalidation messages
	// 	 * anyway, to flush any negative catcache entries that may be
	// 	 * lingering.
	// 	 */
    //     if (!OidIsValid(relId))
    //         AcceptInvalidationMessages();
    //     else if (!nowait)
    //         LockRelationOid(relId, lockmode);
    //     else if (!ConditionalLockRelationOid(relId, lockmode))
    //     {
    //         if (relation->schemaname)
    //             ereport(
    //                 ERROR,
    //                 (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
    //                  errmsg("could not obtain lock on relation \"%s.%s\"", relation->schemaname, relation->relname)));
    //         else
    //             ereport(
    //                 ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("could not obtain lock on relation \"%s\"", relation->relname)));
    //     }

    //     /*
	// 	 * If no invalidation message were processed, we're done!
	// 	 */
    //     if (inval_count == SharedInvalidMessageCounter)
    //         break;

    //     /*
	// 	 * Something may have changed.  Let's repeat the name lookup, to make
	// 	 * sure this name still references the same relation it did
	// 	 * previously.
	// 	 */
    //     retry = true;
    //     oldRelId = relId;
    // }

    // if (!OidIsValid(relId) && !missing_ok)
    // {
    //     if (relation->schemaname)
    //         ereport(
    //             ERROR,
    //             (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s.%s\" does not exist", relation->schemaname, relation->relname)));
    //     else
    //         ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" does not exist", relation->relname)));
    // }
    // return relId;

    PGOid relId = InvalidOid;
    if (relation->schemaname)
    {
        PGOid namespaceId;

        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(relation->schemaname, missing_ok);
        if (missing_ok && !OidIsValid(namespaceId))
            relId = InvalidOid;
        else
            relId = get_relname_relid(relation->relname, namespaceId);
    }
    else
    {
        /* search the namespace path */
        relId = get_relname_relid(relation->relname, InvalidOid);
    }

    return relId;
};

PGRelationPtr RelationProvider::relation_open(PGOid relationId, LOCKMODE lockmode)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    if (lockmode == RowShareLock
        || lockmode == RowExclusiveLock
        || lockmode == ShareUpdateExclusiveLock
        || lockmode == ShareRowExclusiveLock
        || lockmode == AccessExclusiveLock)
    {
        elog(ERROR, "LockMode do not supported yet %u", lockmode);
        return nullptr;
    }
    
    auto it = oid_relation_map.find(relationId);
	if (it == oid_relation_map.end())
    {
	    return nullptr;
    }
    // TODO kindred
    // AccessShareLock, ShareLock, ExclusiveLock
    // it->second->storage_ptr->lockForShare()
	return it->second;
};

void RelationProvider::relation_close(PGRelationPtr relation, LOCKMODE lockmode)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    return;
};

PGRelationPtr RelationProvider::try_heap_open(PGOid relationId, LOCKMODE lockmode, bool noWait)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    return relation_open(relationId, lockmode);
};

bool RelationProvider::IsSystemRelation(PGRelationPtr relation)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    return false;
};

PGOid RelationProvider::get_relname_relid(const char * relname, PGOid relnamespace)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    for (auto pair : oid_relation_map)
    {
        if (pair.second->rd_rel->relname == std::string(relname)
            && pair.second->rd_rel->relnamespace == relnamespace)
        {
            return pair.second->oid;
        }
    }
    
    return InvalidOid;
};

PGOid RelationProvider::LookupNamespaceNoError(const char * nspname)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");
    
    /* check for pg_temp alias */
    // if (strcmp(nspname, "pg_temp") == 0)
    // {
    //     if (OidIsValid(myTempNamespace))
    //     {
    //         InvokeNamespaceSearchHook(myTempNamespace, true);
    //         return myTempNamespace;
    //     }

    //     /*
	// 	 * Since this is used only for looking up existing objects, there is
	// 	 * no point in trying to initialize the temp namespace here; and doing
	// 	 * so might create problems for some callers. Just report "not found".
	// 	 */
    //     return InvalidOid;
    // }

    // return get_namespace_oid(nspname, true);

    for (auto pair : oid_database_map)
    {
        if (pair.second->name == std::string(nspname))
        {
            return pair.second->oid;
        }
    }
    
    return InvalidOid;
};

// PGAttrPtr RelationProvider::SystemAttributeByName(const char * attname, bool relhasoids)
// {
//     // int j;

//     // for (j = 0; j < (int)lengthof(SysAtt); j++)
//     // {
//     //     Form_pg_attribute att = SysAtt[j];

//     //     if (relhasoids || att->attnum != ObjectIdAttributeNumber)
//     //     {
//     //         if (strcmp(NameStr(att->attname), attname) == 0)
//     //             return att;
//     //     }
//     // }

//     // return NULL;

//     for (auto virtual_col : getVirtuals)
//     {

//     }

//     return nullptr;
// };

// PGPolicyPtr RelationProvider::PGPolicyFetch(Oid tbloid)
// {
//     PGPolicyPtr r = nullptr;

// 	return r;
// };

bool RelationProvider::PGPolicyIsReplicated(const PGPolicyPtr policy)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    if (policy == nullptr)
        return false;

    return policy->ptype == POLICYTYPE_REPLICATED;
};

// bool RelationProvider::AttrExistsInRel(PGOid rel_oid, int attr_no)
// {
//     return true;
// };

PGAttrNumber RelationProvider::get_attnum(PGOid relid, const char * attname)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    auto it = oid_relation_map.find(relid);
	if (it == oid_relation_map.end())
	    return InvalidAttrNumber;
	for (const auto& att : it->second->rd_att->attrs)
    {
        if (att->attname == std::string(attname))
        {
            return att->attnum;
        }
    }

    return InvalidAttrNumber;
};

PGOid RelationProvider::get_atttype(PGOid relid, PGAttrNumber attnum)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    // HeapTuple tp;

    // tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
    //     PGOid result;

    //     result = att_tup->atttypid;
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    // else
    //     return InvalidOid;

    auto it = oid_relation_map.find(relid);
	if (it == oid_relation_map.end())
	    return InvalidOid;
	for (const auto& att : it->second->rd_att->attrs)
    {
        if (att->attnum == attnum)
        {
            return att->atttypid;
        }
    }

    return InvalidOid;
};

PGOid RelationProvider::LookupExplicitNamespace(const char * nspname, bool missing_ok)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    // PGOid namespaceId;
    // AclResult aclresult;

    // /* check for pg_temp alias */
    // if (strcmp(nspname, "pg_temp") == 0)
    // {
    //     if (TempNamespaceValid(true))
    //         return myTempNamespace;

    //     /*
	// 	 * Since this is used only for looking up existing objects, there is
	// 	 * no point in trying to initialize the temp namespace here; and doing
	// 	 * so might create problems for some callers --- just fall through.
	// 	 */
    // }

    // namespaceId = get_namespace_oid(nspname, missing_ok);
    // if (missing_ok && !OidIsValid(namespaceId))
    //     return InvalidOid;

    // aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_USAGE);
    // if (aclresult != ACLCHECK_OK)
    //     aclcheck_error(aclresult, ACL_KIND_NAMESPACE, nspname);
    // /* Schema search hook for this lookup */
    // InvokeNamespaceSearchHook(namespaceId, true);

    // return namespaceId;

    PGOid namespaceId = InvalidOid;
    for (auto pair : oid_database_map)
    {
        if (pair.second->name == std::string(nspname))
        {
            namespaceId = pair.second->oid;
        }
    }

    if (missing_ok && !OidIsValid(namespaceId))
    {
        return InvalidOid;
    }

    return namespaceId;
};

std::string RelationProvider::get_namespace_name(PGOid nspid)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    // HeapTuple tp;

    // tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
    //     char * result;

    //     result = pstrdup(NameStr(nsptup->nspname));
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    // else
    //     return NULL;

    for (auto pair : oid_database_map)
    {
        if (pair.second->oid == nspid)
        {
            return pair.second->name;
        }
    }

    return "";
};

PGRelationPtr RelationProvider::heap_open(PGOid relationId, LOCKMODE lockmode)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    PGRelationPtr r;
    
    r = relation_open(relationId, lockmode);
    if (!r)
    {
        elog(ERROR, "Table not find, Oid: %d.", relationId);
        return nullptr;
    }

    if (r->rd_rel->relkind == PG_RELKIND_INDEX)
        ereport(ERROR, (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is an index", RelationGetRelationName(r))));
    else if (r->rd_rel->relkind == PG_RELKIND_COMPOSITE_TYPE)
        ereport(ERROR, (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is a composite type", RelationGetRelationName(r))));

    return r;
};

PGOid RelationProvider::get_rel_type_id(PGOid relid)
{
    if (!is_init) elog(ERROR, "RelationProvider not inited, call Init() first.");

    auto rel_class = getClassByRelOid(relid);
    return rel_class->reltype;
};

}
