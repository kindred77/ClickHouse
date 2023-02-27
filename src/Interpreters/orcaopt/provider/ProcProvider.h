#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{
/*
pg_proc  -> CMDFunctionGPDB
*/
class ProcProvider
{
private:
	using Map = std::map<Oid, PGProcPtr>;

	Map oid_proc_map;

	static std::pair<Oid, PGProcPtr> PROC_INT2PL;
	static std::pair<Oid, PGProcPtr> PROC_INT4PL;
	static std::pair<Oid, PGProcPtr> PROC_INT24PL;
	static std::pair<Oid, PGProcPtr> PROC_INT42PL;
public:
	//explicit ProcProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit ProcProvider();
	
	PGProcPtr getProcByOid(Oid oid) const;

    bool get_func_retset(Oid funcid);
};

}
