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
	using Map = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr>;

	Map oid_cast_map;

	ContextPtr context;

	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT64_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT64_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT64_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT64_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT16_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT16_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT16_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT16_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> BOOL_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> BOOL_TO_STRING;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT32_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT32_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT32_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT32_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT64_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT64_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT64_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> FLOAT64_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT32_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT32_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT32_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> INT32_TO_FLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> DECIMAL64_TO_INT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> DECIMAL64_TO_INT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> DECIMAL64_TO_INT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> DECIMAL64_TO_FLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> DECIMAL64_TO_FLOAT64;
public:
	//explicit CastProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit CastProvider(const ContextPtr& context_);
	
	duckdb_libpgquery::PGCastPtr getCastBySourceTypeAndTargetTypeOid(duckdb_libpgquery::PGOid sourceTypeId, duckdb_libpgquery::PGOid targetTypeId) const;

	static int CAST_OID_ID;
};

}
