#include <Interpreters/orcaopt/CastProvider.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-variable"
#else
#pragma GCC diagnostic ignored "-Wunused-variable"
#endif

namespace DB
{

PGCastPtr CastProvider::getCastBySourceTypeAndTargetTypeOid(Oid sourceTypeId, Oid targetTypeId) const
{
	//TODO
	auto it = oid_cast_map.find(sourceTypeId);
	auto it2 = oid_cast_map.find(targetTypeId);
	if (it == oid_cast_map.end())
	    return {};
	return it->second;
};

}
