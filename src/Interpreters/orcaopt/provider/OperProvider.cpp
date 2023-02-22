#include <Interpreters/orcaopt/provider/OperProvider.h>

using namespace duckdb_libpgquery;

namespace DB
{

// OperProvider::OperProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
// 		: context(std::move(context_)),
// 		  mp(std::move(mp_))
// {
// 	auto operator1= std::make_shared<Form_pg_operator>(Oid(1), "+", Oid(1), Oid(1), 'l', true, Oid(1),
// 		 Oid(1), Oid(1), Oid(1), Oid(1), Oid(1), Oid(1), Oid(1), Oid(1), Oid(1), Oid(1), Oid(1));
// 	oid_oper_map.insert(std::pair<Oid, PGOperatorPtr>(Oid(1), operator1));
// };

PGOperatorPtr
OperProvider::getOperByOID(Oid oid)
{
	auto it = oid_oper_map.find(oid);
	if (it == oid_oper_map.end())
	    return {};
	return it->second;
};

Oid
OperProvider::getOperByName(duckdb_libpgquery::PGList *names, Oid oprleft, Oid oprright)
{
    char * schemaname;
    char * opername;
	DeconstructQualifiedName(names, &schemaname, &opername);

	//TODO find oper with oper name
	return Oid(1);
};

FuncCandidateList
OperProvider::OpernameGetCandidates(PGList * names, char oprkind, bool missing_schema_ok)
{
    FuncCandidateList resultList = NULL;
    char * resultSpace = NULL;
    int nextResult = 0;
    char * schemaname;
    char * opername;
    Oid namespaceId;
    CatCList * catlist;
    int i;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &opername);

    if (schemaname)
    {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_schema_ok);
        if (missing_schema_ok && !OidIsValid(namespaceId))
            return NULL;
    }
    else
    {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

    /* Search syscache by name only */
    catlist = SearchSysCacheList1(OPERNAMENSP, CStringGetDatum(opername));

    /*
	 * In typical scenarios, most if not all of the operators found by the
	 * catcache search will end up getting returned; and there can be quite a
	 * few, for common operator names such as '=' or '+'.  To reduce the time
	 * spent in palloc, we allocate the result space as an array large enough
	 * to hold all the operators.  The original coding of this routine did a
	 * separate palloc for each operator, but profiling revealed that the
	 * pallocs used an unreasonably large fraction of parsing time.
	 */
#define SPACE_PER_OP MAXALIGN(sizeof(struct _FuncCandidateList) + sizeof(Oid))

    if (catlist->n_members > 0)
        resultSpace = palloc(catlist->n_members * SPACE_PER_OP);

    for (i = 0; i < catlist->n_members; i++)
    {
        HeapTuple opertup = &catlist->members[i]->tuple;
        Form_pg_operator operform = (Form_pg_operator)GETSTRUCT(opertup);
        int pathpos = 0;
        FuncCandidateList newResult;

        /* Ignore operators of wrong kind, if specific kind requested */
        if (oprkind && operform->oprkind != oprkind)
            continue;

        if (OidIsValid(namespaceId))
        {
            /* Consider only opers in specified namespace */
            if (operform->oprnamespace != namespaceId)
                continue;
            /* No need to check args, they must all be different */
        }
        else
        {
            /*
			 * Consider only opers that are in the search path and are not in
			 * the temp namespace.
			 */
            ListCell * nsp;

            foreach (nsp, activeSearchPath)
            {
                if (operform->oprnamespace == lfirst_oid(nsp) && operform->oprnamespace != myTempNamespace)
                    break;
                pathpos++;
            }
            if (nsp == NULL)
                continue; /* oper is not in search path */

            /*
			 * Okay, it's in the search path, but does it have the same
			 * arguments as something we already accepted?	If so, keep only
			 * the one that appears earlier in the search path.
			 *
			 * If we have an ordered list from SearchSysCacheList (the normal
			 * case), then any conflicting oper must immediately adjoin this
			 * one in the list, so we only need to look at the newest result
			 * item.  If we have an unordered list, we have to scan the whole
			 * result list.
			 */
            if (resultList)
            {
                FuncCandidateList prevResult;

                if (catlist->ordered)
                {
                    if (operform->oprleft == resultList->args[0] && operform->oprright == resultList->args[1])
                        prevResult = resultList;
                    else
                        prevResult = NULL;
                }
                else
                {
                    for (prevResult = resultList; prevResult; prevResult = prevResult->next)
                    {
                        if (operform->oprleft == prevResult->args[0] && operform->oprright == prevResult->args[1])
                            break;
                    }
                }
                if (prevResult)
                {
                    /* We have a match with a previous result */
                    Assert(pathpos != prevResult->pathpos);
                    if (pathpos > prevResult->pathpos)
                        continue; /* keep previous result */
                    /* replace previous result */
                    prevResult->pathpos = pathpos;
                    prevResult->oid = HeapTupleGetOid(opertup);
                    continue; /* args are same, of course */
                }
            }
        }

        /*
		 * Okay to add it to result list
		 */
        newResult = (FuncCandidateList)(resultSpace + nextResult);
        nextResult += SPACE_PER_OP;

        newResult->pathpos = pathpos;
        newResult->oid = HeapTupleGetOid(opertup);
        newResult->nargs = 2;
        newResult->nvargs = 0;
        newResult->ndargs = 0;
        newResult->argnumbers = NULL;
        newResult->args[0] = operform->oprleft;
        newResult->args[1] = operform->oprright;
        newResult->next = resultList;
        resultList = newResult;
    }

    ReleaseSysCacheList(catlist);

    return resultList;
};

/*
 * get_opcode
 *
 *		Returns the regproc id of the routine used to implement an
 *		operator given the operator oid.
 */
Oid OperProvider::get_opcode(Oid opno)
{
    PGOperatorPtr op = getOperByOID(opno);
    if (op != NULL)
    {
        return op->oprcode;
    }
    else
        return InvalidOid;
};

Oid OperProvider::get_commutator(Oid opno)
{
    PGOperatorPtr op = getOperByOID(opno);
    if (op != NULL)
    {
        return op->oprcom;
    }
    else
        return InvalidOid;
};

bool OperProvider::get_ordering_op_properties(Oid opno, Oid * opfamily, Oid * opcintype, int16 * strategy)
{
    bool result = false;
    CatCList * catlist;
    int i;

    /* ensure outputs are initialized on failure */
    *opfamily = InvalidOid;
    *opcintype = InvalidOid;
    *strategy = 0;

    /*
	 * Search pg_amop to see if the target operator is registered as the "<"
	 * or ">" operator of any btree opfamily.
	 */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    for (i = 0; i < catlist->n_members; i++)
    {
        HeapTuple tuple = &catlist->members[i]->tuple;
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);

        /* must be btree */
        if (aform->amopmethod != BTREE_AM_OID)
            continue;

        if (aform->amopstrategy == BTLessStrategyNumber || aform->amopstrategy == BTGreaterStrategyNumber)
        {
            /* Found it ... should have consistent input types */
            if (aform->amoplefttype == aform->amoprighttype)
            {
                /* Found a suitable opfamily, return info */
                *opfamily = aform->amopfamily;
                *opcintype = aform->amoplefttype;
                *strategy = aform->amopstrategy;
                result = true;
                break;
            }
        }
    }

    ReleaseSysCacheList(catlist);

    return result;
};

Oid OperProvider::get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy)
{
    HeapTuple tp;
    Form_pg_amop amop_tup;
    Oid result;

    tp = SearchSysCache4(
        AMOPSTRATEGY, ObjectIdGetDatum(opfamily), ObjectIdGetDatum(lefttype), ObjectIdGetDatum(righttype), Int16GetDatum(strategy));
    if (!HeapTupleIsValid(tp))
        return InvalidOid;
    amop_tup = (Form_pg_amop)GETSTRUCT(tp);
    result = amop_tup->amopopr;
    ReleaseSysCache(tp);
    return result;
};

Oid OperProvider::get_equality_op_for_ordering_op(Oid opno, bool * reverse)
{
    Oid result = InvalidOid;
    Oid opfamily;
    Oid opcintype;
    int16 strategy;

    /* Find the operator in pg_amop */
    if (get_ordering_op_properties(opno, &opfamily, &opcintype, &strategy))
    {
        /* Found a suitable opfamily, get matching equality operator */
        result = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
        if (reverse)
            *reverse = (strategy == BTGreaterStrategyNumber);
    }

    return result;
};

bool OperProvider::op_hashjoinable(Oid opno, Oid inputtype)
{
    bool result = false;
    // HeapTuple tp;
    // TypeCacheEntry * typentry;

    /* As in op_mergejoinable, let the typcache handle the hard cases */
    /* Eventually we'll need a similar case for record_eq ... */
    // if (opno == ARRAY_EQ_OP)
    // {
    //     typentry = lookup_type_cache(inputtype, TYPECACHE_HASH_PROC);
    //     if (typentry->hash_proc == F_HASH_ARRAY)
    //         result = true;
    // }
    // else
    // {
        PGOperatorPtr op = getOperByOID(opno);
        if (op != NULL)
        {
            result = op->oprcanhash;
        }
    // }
    return result;
};

PGSortGroupOperPtr OperProvider::get_sort_group_operators(Oid type_id)
{
    auto result = std::make_shared<Sort_group_operator>();
    return result;
};

}
