#include <Interpreters/orcaopt/provider/ProcProvider.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

explicit ProcProvider::ProcProvider()
{
	//int2pl
	oid_proc_map.insert(std::pair<Oid, PGProcPtr>(Oid(176), std::make_shared<Form_pg_proc>(
			Oid(176), /*mdname*/ pstrdup("plus"), /*pronamespace*/ Oid(1),
			/*proowner*/ Oid(1), /*prolang*/ Oid(1), /*procost*/ 0.0,
			/*prorows*/ 0.0, /*provariadic*/ Oid(0), /*protransform*/ Oid(1),
			/*proisagg*/ false, /*proiswindow*/ false, /*prosecdef*/ false,
			/*proleakproof*/ false, /*proisstrict*/ true, /*proretset*/ false,
			/*provolatile*/ 'i', /*pronargs*/ 2, /*pronargdefaults*/ 0,
			/*prorettype*/ Oid(1), /*proargtypes*/ oidvector())));
	
	//int4pl
	oid_proc_map.insert(std::pair<Oid, PGProcPtr>(Oid(177), std::make_shared<Form_pg_proc>(
			Oid(177), /*mdname*/ pstrdup("plus"), /*pronamespace*/ Oid(1),
			/*proowner*/ Oid(1), /*prolang*/ Oid(1), /*procost*/ 0.0,
			/*prorows*/ 0.0, /*provariadic*/ Oid(0), /*protransform*/ Oid(1),
			/*proisagg*/ false, /*proiswindow*/ false, /*prosecdef*/ false,
			/*proleakproof*/ false, /*proisstrict*/ true, /*proretset*/ false,
			/*provolatile*/ 'i', /*pronargs*/ 2, /*pronargdefaults*/ 0,
			/*prorettype*/ Oid(1), /*proargtypes*/ oidvector())));

	//int24pl
	oid_proc_map.insert(std::pair<Oid, PGProcPtr>(Oid(178), std::make_shared<Form_pg_proc>(
			Oid(178), /*mdname*/ pstrdup("plus"), /*pronamespace*/ Oid(1),
			/*proowner*/ Oid(1), /*prolang*/ Oid(1), /*procost*/ 0.0,
			/*prorows*/ 0.0, /*provariadic*/ Oid(0), /*protransform*/ Oid(1),
			/*proisagg*/ false, /*proiswindow*/ false, /*prosecdef*/ false,
			/*proleakproof*/ false, /*proisstrict*/ true, /*proretset*/ false,
			/*provolatile*/ 'i', /*pronargs*/ 2, /*pronargdefaults*/ 0,
			/*prorettype*/ Oid(1), /*proargtypes*/ oidvector())));

	//int42pl
	oid_proc_map.insert(std::pair<Oid, PGProcPtr>(Oid(179), std::make_shared<Form_pg_proc>(
			Oid(179), /*mdname*/ pstrdup("plus"), /*pronamespace*/ Oid(1),
			/*proowner*/ Oid(1), /*prolang*/ Oid(1), /*procost*/ 0.0,
			/*prorows*/ 0.0, /*provariadic*/ Oid(0), /*protransform*/ Oid(1),
			/*proisagg*/ false, /*proiswindow*/ false, /*prosecdef*/ false,
			/*proleakproof*/ false, /*proisstrict*/ true, /*proretset*/ false,
			/*provolatile*/ 'i', /*pronargs*/ 2, /*pronargdefaults*/ 0,
			/*prorettype*/ Oid(1), /*proargtypes*/ oidvector())));
};

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
