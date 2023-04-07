#pragma once

#include <Interpreters/orcaopt/parser_common.h>

#include <naucrates/md/CMDName.h>
#include <naucrates/md/IMDFunction.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{
using IMDFunctionPtr = std::shared_ptr<const gpmd::IMDFunction>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class FunctionProvider
{
private:
    using Map = std::map<OID, IMDFunctionPtr>;

    Map oid_fun_map;
    ContextPtr context;
    gpos::CMemoryPool * mp;

    gpmd::CMDName * CreateMDName(const char * name_str);

public:
    //explicit FunctionProvider(gpos::CMemoryPool *mp_, ContextPtr context);
    explicit FunctionProvider(const ContextPtr& context_);
    IMDFunctionPtr getFunctionByOID(OID oid) const;

    // Datum OidFunctionCall2(Oid functionId, Datum arg1, Datum arg2);

    // Datum OidFunctionCall1Coll(Oid functionId, Datum arg1);

    // Datum OidFunctionCall1Coll(Oid functionId, Oid collation, Datum arg1);

    // Datum OidFunctionCall1_DatumArr(Oid functionId, Datum * datums);

    Datum OidInputFunctionCall(Oid functionId, const char * str, Oid typioparam, int32 typmod);


    FuncCandidateListPtr FuncnameGetCandidates(
        duckdb_libpgquery::PGList * names,
        int nargs,
        duckdb_libpgquery::PGList * argnames,
        bool expand_variadic,
        bool expand_defaults,
        bool missing_ok);

    duckdb_libpgquery::PGList * SystemFuncName(const char * name);

    char * get_func_result_name(Oid functionId);
};

}
