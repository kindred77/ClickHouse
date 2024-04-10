#pragma once

#include <common/parser_common.hpp>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{

/*
select
pc.castsource||'-'||(select pt.typname from pg_type pt where pt.oid=pc.castsource) src_typ_name,
pc.casttarget||'-'||(select pt.typname from pg_type pt where pt.oid=pc.casttarget) tgt_typ_name,
pc.castfunc||'-'||(select pp.proname from pg_proc pp where pp.oid=pc.castfunc) cast_func_name,
castcontext,
castmethod
from pg_cast pc
*/

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class CastProvider
{
private:

	using OidCastMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr>;
    static OidCastMap oid_cast_map;

	ContextPtr context;

	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT64_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT64_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT64_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT64_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT16_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT16_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT16_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT16_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_BOOL_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_BOOL_TO_STRING;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT32_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT32_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT32_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT32_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT64_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT64_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT64_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT64_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT32_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT32_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT32_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT32_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_DECIMAL64_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_DECIMAL64_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_DECIMAL64_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_DECIMAL64_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_DECIMAL64_TO_FLOAT64;
public:
	//explicit CastProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	//explicit CastProvider(const ContextPtr& context_);
	
	static duckdb_libpgquery::PGCastPtr getCastBySourceTypeAndTargetTypeOid(duckdb_libpgquery::PGOid sourceTypeId, duckdb_libpgquery::PGOid targetTypeId);

	static int CAST_OID_ID;
};

}
