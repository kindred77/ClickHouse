#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/orcaopt/walkers.h>
#include <Storages/IStorage.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-label"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-label"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

StoragePtr
RelationProvider::getStorageByOID(Oid oid) const
{
	auto it = oid_storageid_map.find(oid);
	if (it == oid_storageid_map.end())
	    return {};
	return it->second;
};

// std::optional<std::tuple<Oid, StoragePtr, char> >
// RelationProvider::getPairByDBAndTableName(const String & database_name, const String & table_name) const
// {
// 	auto it = oid_storageid_map.find(oid);
// 	if (it == oid_storageid_map.end())
// 	    return nullptr;
// 	return {it->first, it->second, 'r'};
// }

std::string RelationProvider::get_database_name(Oid dbid) const
{
    // HeapTuple dbtuple;
    // std::string result = "";

    // dbtuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
    // if (HeapTupleIsValid(dbtuple))
    // {
    //     result = pstrdup(NameStr(((Form_pg_database)GETSTRUCT(dbtuple))->datname));
    //     ReleaseSysCache(dbtuple);
    // }
    // else
    //     result = NULL;

    // return result;

    return "";
};

char RelationProvider::get_rel_relkind(Oid relid) const
{
    // HeapTuple tp;

    // tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
    //     char result;

    //     result = reltup->relkind;
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    // else
    //     return '\0';

    return '\0';
};

std::string RelationProvider::get_attname(Oid relid, PGAttrNumber attnum) const
{
    std::string result = "";

    return result;
};

const char * RelationProvider::get_rel_name(Oid relid)
{
    return "";
};

PGAttrPtr RelationProvider::get_att_by_reloid_attnum(Oid relid, PGAttrNumber attnum) const
{
    PGAttrPtr attr = nullptr;

    return attr;
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

Oid RelationProvider::RangeVarGetRelidExtended(
        const PGRangeVar * relation, LOCKMODE lockmode, bool missing_ok,
		bool nowait, RangeVarGetRelidCallback callback, void * callback_arg)
{
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

    return InvalidOid;
};

PGRelationPtr RelationProvider::relation_open(Oid relationId, LOCKMODE lockmode)
{
    PGRelationPtr r = nullptr;

	return r;
};

void RelationProvider::relation_close(PGRelationPtr relation, LOCKMODE lockmode)
{

};

PGRelationPtr RelationProvider::try_heap_open(Oid relationId, LOCKMODE lockmode, bool noWait)
{
    return nullptr;
};

bool RelationProvider::IsSystemRelation(PGRelationPtr relation)
{
    return false;
};

PGRelationPtr RelationProvider::parserOpenTable(PGParseState * pstate, const PGRangeVar * relation, int lockmode, bool * lockUpgraded)
{
    PGRelationPtr r = nullptr;

	return r;
};

Oid RelationProvider::get_relname_relid(const char * relname, Oid relnamespace)
{
    // return GetSysCacheOid2(RELNAMENSP, PointerGetDatum(relname), ObjectIdGetDatum(relnamespace));

    return InvalidOid;
};

Oid RelationProvider::LookupNamespaceNoError(const char * nspname)
{
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

    return InvalidOid;
};

PGAttrPtr RelationProvider::SystemAttributeByName(const char * attname, bool relhasoids)
{
    // int j;

    // for (j = 0; j < (int)lengthof(SysAtt); j++)
    // {
    //     Form_pg_attribute att = SysAtt[j];

    //     if (relhasoids || att->attnum != ObjectIdAttributeNumber)
    //     {
    //         if (strcmp(NameStr(att->attname), attname) == 0)
    //             return att;
    //     }
    // }

    // return NULL;

    return nullptr;
};

PGPolicyPtr RelationProvider::PGPolicyFetch(Oid tbloid)
{
    PGPolicyPtr r = nullptr;

	return r;
};

bool RelationProvider::PGPolicyIsReplicated(const PGPolicyPtr policy)
{
    if (policy == nullptr)
        return false;

    return policy->ptype == POLICYTYPE_REPLICATED;
};

bool RelationProvider::AttrExistsInRel(Oid rel_oid, int attr_no)
{
    return true;
};

PGAttrNumber RelationProvider::get_attnum(Oid relid, const char * attname)
{
    // HeapTuple tp;

    // tp = SearchSysCacheAttName(relid, attname);
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
    //     AttrNumber result;

    //     result = att_tup->attnum;
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    // else
    //     return InvalidAttrNumber;

    return InvalidAttrNumber;
};

Oid RelationProvider::get_atttype(Oid relid, PGAttrNumber attnum)
{
    // HeapTuple tp;

    // tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
    //     Oid result;

    //     result = att_tup->atttypid;
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    // else
    //     return InvalidOid;

    return InvalidOid;
};

Oid RelationProvider::LookupExplicitNamespace(const char * nspname, bool missing_ok)
{
    // Oid namespaceId;
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

    return InvalidOid;
};

std::string RelationProvider::get_namespace_name(Oid nspid)
{
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

    return pstrdup("");
};

PGRelationPtr RelationProvider::heap_open(Oid relationId, LOCKMODE lockmode)
{
    PGRelationPtr r;

    r = relation_open(relationId, lockmode);

    if (r->rd_rel->relkind == PG_RELKIND_INDEX)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is an index", RelationGetRelationName(r))));
    else if (r->rd_rel->relkind == PG_RELKIND_COMPOSITE_TYPE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is a composite type", RelationGetRelationName(r))));

    return r;
};

}
