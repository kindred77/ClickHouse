#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{
class AggProvider
{
private:
	using Map = std::map<Oid, PGAggPtr>;

	Map oid_agg_map;
public:
	//explicit AggProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit AggProvider();
	
	PGAggPtr getAggByFuncOid(Oid func_oid) const;
};

}
