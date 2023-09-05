#pragma once

#include <common/parser_common.hpp>

#include <naucrates/md/CMDName.h>
#include <naucrates/md/IMDFunction.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{
using IMDFunctionPtr = std::shared_ptr<const gpmd::IMDFunction>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class ProcProvider;
using ProcProviderPtr = std::shared_ptr<ProcProvider>;

using String = std::string;

class FunctionProvider
{
private:
    //using Map = std::map<OID, IMDFunctionPtr>;

    //Map oid_fun_map;
    ContextPtr context;
    gpos::CMemoryPool * mp;
    ProcProviderPtr proc_provider;
    gpmd::CMDName * CreateMDName(const char * name_str);

public:
    //explicit FunctionProvider(gpos::CMemoryPool *mp_, ContextPtr context);
    explicit FunctionProvider(const ContextPtr& context_);
    //IMDFunctionPtr getFunctionByOID(OID oid) const;

    // Datum OidFunctionCall2(Oid functionId, Datum arg1, Datum arg2);

    // Datum OidFunctionCall1Coll(Oid functionId, Datum arg1);

    // Datum OidFunctionCall1Coll(Oid functionId, Oid collation, Datum arg1);

    // Datum OidFunctionCall1_DatumArr(Oid functionId, Datum * datums);

    duckdb_libpgquery::PGDatum OidInputFunctionCall(duckdb_libpgquery::PGOid functionId, const char * str, duckdb_libpgquery::PGOid typioparam, duckdb_libpgquery::int32 typmod);

    int get_func_arg_info(const duckdb_libpgquery::PGProcPtr& procTup, std::vector<duckdb_libpgquery::PGOid>& p_argtypes, std::vector<String>& p_argnames, std::vector<char>& p_argmodes);

    bool MatchNamedCall(const duckdb_libpgquery::PGProcPtr& proctup, int nargs, duckdb_libpgquery::PGList * argnames, int ** argnumbers);

    duckdb_libpgquery::FuncCandidateListPtr FuncnameGetCandidates(
        duckdb_libpgquery::PGList * names,
        int nargs,
        duckdb_libpgquery::PGList * argnames,
        bool expand_variadic,
        bool expand_defaults,
        bool missing_ok);

    duckdb_libpgquery::PGList * SystemFuncName(const char * name);

    String get_func_result_name(duckdb_libpgquery::PGOid functionId);
};

}
