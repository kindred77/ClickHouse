#include <Interpreters/orcaopt/provider/ProcProvider.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGProcPtr ProcProvider::getProcByOid(Oid oid) const
{
	auto it = oid_proc_map.find(oid);
	if (it == oid_proc_map.end())
	    return {};
	return it->second;
};

bool ProcProvider::get_func_retset(Oid funcid)
{
	PGProcPtr tp = getProcByOid(funcid);
	if (tp == NULL)
	{
		elog(ERROR, "cache lookup failed for function %u", funcid);
	}

	return tp->proretset;
};

}
