
#pragma once

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/metagen/common.h>

#include <unordered_map>

namespace DB
{

class Cast;
using CastPtr = std::shared_ptr<Cast>;

class Cast
{
private:
    static void initVarName(PGConnectionPtr conn, CastPtr oper);
public:
    duckdb_libpgquery::PGOid oid;
    duckdb_libpgquery::PGOid castsource;
    duckdb_libpgquery::PGOid casttarget;
    duckdb_libpgquery::PGOid castfunc;
    std::string castcontext;
    std::string castmethod;

    //prop for output
    std::string var_name;

    static std::unordered_map<std::string, CastPtr> cast_map;

    static bool init(PGConnectionPtr conn, duckdb_libpgquery::PGOid source_oid, duckdb_libpgquery::PGOid target_oid);

    static void output();
};

}
