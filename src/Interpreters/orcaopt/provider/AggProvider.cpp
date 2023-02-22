#include <Interpreters/orcaopt/provider/AggProvider.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGAggPtr AggProvider::getAggByFuncOid(Oid func_oid) const
{
	//TODO kindred
	auto it = oid_agg_map.find(func_oid);
	if (it == oid_agg_map.end())
	    return {};
	return it->second;
};

}
