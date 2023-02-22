#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{
class CastProvider
{
private:
	using Map = std::map<Oid, PGCastPtr>;

	Map oid_cast_map;
public:
	//explicit CastProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit CastProvider();
	
	PGCastPtr getCastBySourceTypeAndTargetTypeOid(Oid sourceTypeId, Oid targetTypeId) const;
};

}
