#pragma once

#include <common/parser_common.hpp>
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
	using OidAggMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGAggPtr>;
    static OidAggMap oid_agg_map;
	//ContextPtr context;
public:
	//explicit AggProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	//explicit AggProvider(const ContextPtr& context_);
	
	static duckdb_libpgquery::PGAggPtr getAggByFuncOid(duckdb_libpgquery::PGOid func_oid);
};

}
