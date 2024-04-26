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

	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_NUMERIC_TO_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_NUMERIC_TO_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_NUMERIC_TO_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_BOOL_TO_TEXT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT4_TO_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT2_TO_INT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT4_TO_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT4_TO_INT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT8_TO_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT8_TO_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT8_TO_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT8_TO_INT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT8_TO_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_NUMERIC_TO_INT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT4_TO_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_NUMERIC_TO_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT4_TO_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT2_TO_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_BOOL_TO_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT4_TO_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT2_TO_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT2_TO_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT8_TO_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT4_TO_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT4_TO_INT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_FLOAT8_TO_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGCastPtr> CAST_INT8_TO_INT2;

public:
	//explicit CastProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	//explicit CastProvider(const ContextPtr& context_);
	
	static duckdb_libpgquery::PGCastPtr getCastBySourceTypeAndTargetTypeOid(duckdb_libpgquery::PGOid sourceTypeId, duckdb_libpgquery::PGOid targetTypeId);

	static int CAST_OID_ID;
};

}
