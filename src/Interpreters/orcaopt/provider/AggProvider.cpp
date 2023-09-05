#include <Interpreters/orcaopt/provider/AggProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

AggProvider::AggProvider(const ContextPtr& context_) : context(context_)
{

};

PGAggPtr AggProvider::getAggByFuncOid(PGOid func_oid) const
{
	//TODO kindred
	auto it = oid_agg_map.find(func_oid);
	if (it == oid_agg_map.end())
	    return {};
	return it->second;
};

}
