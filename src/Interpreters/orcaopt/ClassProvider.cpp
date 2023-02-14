#include <Interpreters/orcaopt/ClassProvider.h>

namespace DB
{

PGClassPtr ClassProvider::getClassByRelOid(Oid oid) const
{
	auto it = oid_class_map.find(oid);
	if (it == oid_class_map.end())
	    return {};
	return it->second;
};

}
