#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{
class ClassProvider
{
private:
	using Map = std::map<Oid, PGClassPtr>;

	Map oid_class_map;
public:
	explicit ClassProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	
	PGClassPtr getClassByRelOid(Oid oid) const;
};

}
