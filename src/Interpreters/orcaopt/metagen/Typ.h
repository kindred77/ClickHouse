
#pragma once

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/metagen/common.h>

#include <unordered_map>

namespace DB
{

class Typ;
using TypPtr = std::shared_ptr<Typ>;

class Typ
{
private:
    static void initVarName(PGConnectionPtr conn, TypPtr agg);
public:
    duckdb_libpgquery::PGOid oid;
    std::string typname;
    duckdb_libpgquery::PGOid typnamespace;
    duckdb_libpgquery::PGOid typowner;
    int typlen;
    bool typbyval;
    std::string typtype;
    std::string typcategory;
    bool typispreferred;
    bool typisdefined;
    std::string typdelim;
    duckdb_libpgquery::PGOid typrelid;
    duckdb_libpgquery::PGOid typelem;
    duckdb_libpgquery::PGOid typarray;
    duckdb_libpgquery::PGOid typinput;
    duckdb_libpgquery::PGOid typoutput;
    duckdb_libpgquery::PGOid typreceive;
    duckdb_libpgquery::PGOid typsend;
    duckdb_libpgquery::PGOid typmodin;
    duckdb_libpgquery::PGOid typmodout;
    duckdb_libpgquery::PGOid typanalyze;
    std::string typalign;
    std::string typstorage;
    bool typnotnull;
    duckdb_libpgquery::PGOid typbasetype;
    int typtypmod;
    int typndims;
    duckdb_libpgquery::PGOid typcollation;
    duckdb_libpgquery::PGOid lt_opr;
    duckdb_libpgquery::PGOid eq_opr;
    duckdb_libpgquery::PGOid gt_opr;
    duckdb_libpgquery::PGOid hash_proc;
    duckdb_libpgquery::PGOid cmp_proc;

    //prop for output
    std::string var_name;

    static std::unordered_map<duckdb_libpgquery::PGOid, TypPtr> typ_map;

    static bool init(PGConnectionPtr conn, duckdb_libpgquery::PGOid oid);

    static void output();
};

}
