
#pragma once

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/metagen/common.h>

#include <unordered_map>

namespace DB
{

class Proc;
using ProcPtr = std::shared_ptr<Proc>;

class Proc
{
private:
    static void initVarName(PGConnectionPtr conn, ProcPtr oper);
    static void initVarName2(PGConnectionPtr conn, ProcPtr oper);
public:
    duckdb_libpgquery::PGOid aggfnoid;

    duckdb_libpgquery::PGOid oid;
    std::string proname;
    duckdb_libpgquery::PGOid pronamespace;
    duckdb_libpgquery::PGOid proowner;
    duckdb_libpgquery::PGOid prolang;
    float procost;
    float prorows;
    duckdb_libpgquery::PGOid provariadic;
    duckdb_libpgquery::PGOid protransform;
    bool proisagg;
    bool proiswindow;
    bool prosecdef;
    bool proleakproof;
    bool proisstrict;
    bool proretset;
    std::string provolatile;
    int pronargs;
    int pronargdefaults;
    duckdb_libpgquery::PGOid prorettype;
    std::vector<duckdb_libpgquery::PGOid> proargtypes;

    //prop for output
    std::string var_name;

    static std::unordered_map<std::string, ProcPtr> proc_map2; 
    static std::unordered_map<duckdb_libpgquery::PGOid, ProcPtr> proc_map;

    static bool init(PGConnectionPtr conn, duckdb_libpgquery::PGOid oid);

    static void output();
};

}
