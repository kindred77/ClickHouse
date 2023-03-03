#include <Interpreters/orcaopt/provider/ClassProvider.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGClassPtr ClassProvider::getClassByRelOid(Oid oid) const
{
	auto it = oid_class_map.find(oid);
	if (it == oid_class_map.end())
	    return {};
	return it->second;
};

bool ClassProvider::has_subclass(Oid relationId)
{
    // HeapTuple tuple;
    // bool result;

    // tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
    // if (!HeapTupleIsValid(tuple))
    //     elog(ERROR, "cache lookup failed for relation %u", relationId);

    // result = ((Form_pg_class)GETSTRUCT(tuple))->relhassubclass;
    // ReleaseSysCache(tuple);
    // return result;

	PGClassPtr tuple = getClassByRelOid(relationId);
	if (tuple == nullptr)
	{
		elog(ERROR, "cache lookup failed for relation %u", relationId);
		return false;
	}

	return tuple->relhassubclass;
};

}
