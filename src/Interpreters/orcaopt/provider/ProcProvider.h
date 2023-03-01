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
	static std::pair<Oid, PGProcPtr> PROC_INT2MI;
	static std::pair<Oid, PGProcPtr> PROC_INT4MI;
	static std::pair<Oid, PGProcPtr> PROC_INT24MI;
	static std::pair<Oid, PGProcPtr> PROC_INT42MI;
	static std::pair<Oid, PGProcPtr> PROC_INT2MUL;
	static std::pair<Oid, PGProcPtr> PROC_INT4MUL;
	static std::pair<Oid, PGProcPtr> PROC_INT24MUL;
	static std::pair<Oid, PGProcPtr> PROC_INT42MUL;
	static std::pair<Oid, PGProcPtr> PROC_INT2DIV;
	static std::pair<Oid, PGProcPtr> PROC_INT4DIV;
	static std::pair<Oid, PGProcPtr> PROC_INT24DIV;
	static std::pair<Oid, PGProcPtr> PROC_INT42DIV;
public:
	//explicit ProcProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit ProcProvider();
	
	PGProcPtr getProcByOid(Oid oid) const;

    bool get_func_retset(Oid funcid);
};

}
