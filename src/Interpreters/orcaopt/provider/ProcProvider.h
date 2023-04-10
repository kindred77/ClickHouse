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

class Context;
using ContextPtr = std::shared_ptr<const Context>;

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

	static std::pair<Oid, PGProcPtr> PROC_INT64TOINT16;
	static std::pair<Oid, PGProcPtr> PROC_INT64TOINT32;
	static std::pair<Oid, PGProcPtr> PROC_INT64TOFLOAT32;
	static std::pair<Oid, PGProcPtr> PROC_INT64TOFLOAT64;
	static std::pair<Oid, PGProcPtr> PROC_INT16TOINT64;
	static std::pair<Oid, PGProcPtr> PROC_INT16TOINT32;
	static std::pair<Oid, PGProcPtr> PROC_INT16TOFLOAT32;
	static std::pair<Oid, PGProcPtr> PROC_INT16TOFLOAT64;
	static std::pair<Oid, PGProcPtr> PROC_BOOLTOINT32;
	static std::pair<Oid, PGProcPtr> PROC_BOOLTOSTRING;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT32TOINT64;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT32TOINT16;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT32TOINT32;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT32TOFLOAT64;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT64TOINT64;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT64TOINT16;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT64TOINT32;
	static std::pair<Oid, PGProcPtr> PROC_FLOAT64TOFLOAT32;
	static std::pair<Oid, PGProcPtr> PROC_INT32TOINT64;
	static std::pair<Oid, PGProcPtr> PROC_INT32TOINT16;
	static std::pair<Oid, PGProcPtr> PROC_INT32TOFLOAT32;
	static std::pair<Oid, PGProcPtr> PROC_INT32TOFLOAT64;
	static std::pair<Oid, PGProcPtr> PROC_DECIMAL64TOINT64;
	static std::pair<Oid, PGProcPtr> PROC_DECIMAL64TOINT16;
	static std::pair<Oid, PGProcPtr> PROC_DECIMAL64TOINT32;
	static std::pair<Oid, PGProcPtr> PROC_DECIMAL64TOFLOAT64;

	ContextPtr context;
public:
	//explicit ProcProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit ProcProvider(const ContextPtr& context_);
	
	PGProcPtr getProcByOid(Oid oid) const;

	std::unique_ptr<std::vector<PGProcPtr>> search_procs_by_name(const std::string & func_name);

    bool get_func_retset(Oid funcid);
};

}
