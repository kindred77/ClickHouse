
#pragma once

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/metagen/common.h>

#include <unordered_map>

namespace DB
{

class Oper;
using OperPtr = std::shared_ptr<Oper>;

class Oper
{
private:
    static void initVarName(PGConnectionPtr conn, OperPtr oper);
public:
    duckdb_libpgquery::PGOid oid;
    std::string oprname;
    duckdb_libpgquery::PGOid oprnamespace;
    duckdb_libpgquery::PGOid oprowner;
    std::string oprkind;
    bool oprcanmerge;
    bool oprcanhash;
    duckdb_libpgquery::PGOid oprleft;
    duckdb_libpgquery::PGOid oprright;
    duckdb_libpgquery::PGOid oprresult;
    duckdb_libpgquery::PGOid oprcom;
    duckdb_libpgquery::PGOid oprnegate;
    duckdb_libpgquery::PGOid oprcode;
    duckdb_libpgquery::PGOid oprrest;
    duckdb_libpgquery::PGOid oprjoin;

    //prop for output
    std::string var_name;

    static std::unordered_map<duckdb_libpgquery::PGOid, OperPtr> oper_map;

    static bool init(PGConnectionPtr conn, duckdb_libpgquery::PGOid oid);

    static void output();
};

}
