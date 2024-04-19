
#pragma once

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/metagen/common.h>

#include <unordered_map>

namespace DB
{

class Agg;
using AggPtr = std::shared_ptr<Agg>;

class Agg
{
public:
    duckdb_libpgquery::PGOid aggfnoid;
    std::string aggkind;
    int aggnumdirectargs;
    duckdb_libpgquery::PGOid aggtransfn;
    duckdb_libpgquery::PGOid aggfinalfn;
    duckdb_libpgquery::PGOid aggcombinefn;
    duckdb_libpgquery::PGOid aggserialfn;
    duckdb_libpgquery::PGOid aggdeserialfn;
    duckdb_libpgquery::PGOid aggmtransfn;
    duckdb_libpgquery::PGOid aggminvtransfn;
    duckdb_libpgquery::PGOid aggmfinalfn;
    bool aggfinalextra;
    bool aggmfinalextra;
    duckdb_libpgquery::PGOid aggsortop;
    duckdb_libpgquery::PGOid aggtranstype;
    long aggtransspace;
    duckdb_libpgquery::PGOid aggmtranstype;
    long aggmtransspace;
    std::string agginitval;
    std::string aggminitval;

    static std::unordered_map<duckdb_libpgquery::PGOid, AggPtr> agg_map;

    static bool init(PGConnectionPtr conn, duckdb_libpgquery::PGOid oid);
};

}
