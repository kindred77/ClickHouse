#pragma once

#include <common/parser_common.hpp>
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
	using OidProcMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr>;
	static OidProcMap oid_proc_map;

	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42DIV;

	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT64TOINT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT64TOINT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT64TOFLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT64TOFLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT16TOINT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT16TOINT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT16TOFLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT16TOFLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLTOINT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLTOSTRING;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT32TOINT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT32TOINT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT32TOINT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT32TOFLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT64TOINT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT64TOINT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT64TOINT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT64TOFLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT32TOINT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT32TOINT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT32TOFLOAT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT32TOFLOAT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DECIMAL64TOINT64;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DECIMAL64TOINT16;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DECIMAL64TOINT32;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DECIMAL64TOFLOAT64;

	//ContextPtr context;
public:
	//explicit ProcProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	//explicit ProcProvider(const ContextPtr& context_);
	
	static duckdb_libpgquery::PGProcPtr getProcByOid(duckdb_libpgquery::PGOid oid);

	static std::optional<std::string> get_func_name(duckdb_libpgquery::PGOid oid);

	static std::unique_ptr<std::vector<duckdb_libpgquery::PGProcPtr>> search_procs_by_name(const std::string & func_name);

    static bool get_func_retset(duckdb_libpgquery::PGOid funcid);
};

}
