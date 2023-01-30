#include <Interpreters/orcaopt/pgopt_hawq/FuncParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

bool FuncParser::typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId)
{
    bool result = false;
    Oid relid;
    Relation inhrel;
    PGList *visited, *queue;
    PGListCell * queue_item;

    if (!ISCOMPLEX(subclassTypeId) || !ISCOMPLEX(superclassTypeId))
        return false;
    relid = typeidTypeRelid(subclassTypeId);
    if (relid == InvalidOid)
        return false;

    /*
	 * Begin the search at the relation itself, so add relid to the queue.
	 */
    queue = list_make1_oid(relid);
    visited = NIL;

    inhrel = heap_open(InheritsRelationId, AccessShareLock);

    /*
	 * Use queue to do a breadth-first traversal of the inheritance graph from
	 * the relid supplied up to the root.  Notice that we append to the queue
	 * inside the loop --- this is okay because the foreach() macro doesn't
	 * advance queue_item until the next loop iteration begins.
	 */
    foreach (queue_item, queue)
    {
        Oid this_relid = lfirst_oid(queue_item);
        cqContext * pcqCtx;
        cqContext cqc;
        HeapTuple inhtup;

        /* If we've seen this relid already, skip it */
        if (list_member_oid(visited, this_relid))
            continue;

        /*
		 * Okay, this is a not-yet-seen relid. Add it to the list of
		 * already-visited OIDs, then find all the types this relid inherits
		 * from and add them to the queue. The one exception is we don't add
		 * the original relation to 'visited'.
		 */
        if (queue_item != list_head(queue))
            visited = lappend_oid(visited, this_relid);

        pcqCtx = caql_beginscan(
            caql_addrel(cqclr(&cqc), inhrel),
            cql("SELECT * FROM pg_inherits "
                " WHERE inhrelid = :1 ",
                ObjectIdGetDatum(this_relid)));

        while (HeapTupleIsValid(inhtup = caql_getnext(pcqCtx)))
        {
            Form_pg_inherits inh = (Form_pg_inherits)GETSTRUCT(inhtup);
            Oid inhparent = inh->inhparent;

            /* If this is the target superclass, we're done */
            if (get_rel_type_id(inhparent) == superclassTypeId)
            {
                result = true;
                break;
            }

            /* Else add to queue */
            queue = lappend_oid(queue, inhparent);
        }

        caql_endscan(pcqCtx);

        if (result)
            break;
    }

    heap_close(inhrel, AccessShareLock);

    list_free(visited);
    list_free(queue);

    return result;
};

}