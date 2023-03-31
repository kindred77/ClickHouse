#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class AggProvider
{
private:
	using Map = std::map<Oid, PGAggPtr>;

	Map oid_agg_map;
	ContextPtr context;
public:
	//explicit AggProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit AggProvider(const ContextPtr& context_);
	
	PGAggPtr getAggByFuncOid(Oid func_oid) const;
};

}
