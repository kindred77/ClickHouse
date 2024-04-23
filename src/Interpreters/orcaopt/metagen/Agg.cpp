#include <Interpreters/orcaopt/metagen/Agg.h>
#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Proc.h>
#include <Interpreters/orcaopt/metagen/Oper.h>
#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, AggPtr> Agg::agg_map;

bool Agg::init(PGConnectionPtr conn, PGOid oid)
{
    if (oid == InvalidOid) return false;
    if (agg_map.count(oid) > 0) return true;
    std::vector<PGOid> tobeInited_types;
    std::vector<PGOid> tobeInited_procs;
    std::vector<PGOid> tobeInited_opers;
    try
    {
        if (!conn->is_open())
        {
            std::cout << "db not opened." << std::endl;
            return 1;
        }

        std::string sql = "select aggfnoid,aggkind,aggnumdirectargs,aggtransfn,aggfinalfn,aggcombinefn,aggserialfn,aggdeserialfn,aggmtransfn,aggminvtransfn,aggmfinalfn,"
                    "aggfinalextra,aggmfinalextra,aggsortop,aggtranstype,aggtransspace,aggmtranstype,aggmtransspace,agginitval,aggminitval from pg_aggregate where aggfnoid=$1";
        //array_parser parser;
        //parser.add(oid);
        work worker(*conn.get());
        result resp = worker.exec_params(sql.c_str(), oid);
        //result resp = worker.exec(sql.c_str());
        //result resp = conn->prepare(sql.c_str());
        //resp.bind(oid);
        //resp.exec();
        for (auto i =0; i < resp.size(); ++i)
        {
            //std::cout << resp[i]["typname"] << "----" << resp[i]["typlen"] << "----" << typeid(resp[i][1]).name() << std::endl;
            auto agg = std::make_shared<Agg>();
            agg->aggfnoid = oid;
            agg->aggkind = resp[i]["aggkind"].as<std::string>();
            agg->aggnumdirectargs = resp[i]["aggnumdirectargs"].as<int>();
            agg->aggtransfn = resp[i]["aggtransfn"].as<PGOid>();
            agg->aggfinalfn = resp[i]["aggfinalfn"].as<PGOid>();
            agg->aggcombinefn = resp[i]["aggcombinefn"].as<PGOid>();
            agg->aggserialfn = resp[i]["aggserialfn"].as<PGOid>();
            agg->aggdeserialfn = resp[i]["aggdeserialfn"].as<PGOid>();
            agg->aggmtransfn = resp[i]["aggmtransfn"].as<PGOid>();
            agg->aggminvtransfn = resp[i]["aggminvtransfn"].as<PGOid>();
            agg->aggmfinalfn = resp[i]["aggmfinalfn"].as<PGOid>();
            agg->aggfinalextra = resp[i]["aggfinalextra"].as<bool>();
            agg->aggmfinalextra = resp[i]["aggmfinalextra"].as<bool>();
            agg->aggsortop = resp[i]["aggsortop"].as<PGOid>();
            agg->aggtranstype = resp[i]["aggtranstype"].as<PGOid>();
            agg->aggtransspace = resp[i]["aggtransspace"].as<long>();
            agg->aggmtranstype = resp[i]["aggmtranstype"].as<PGOid>();
            agg->aggmtransspace = resp[i]["aggmtransspace"].as<long>();
            agg->agginitval = resp[i]["agginitval"].as<std::string>();
            agg->aggminitval = resp[i]["aggminitval"].as<std::string>();

            agg_map.insert({oid, agg});

            tobeInited_types.push_back(agg->aggtranstype);
            tobeInited_types.push_back(agg->aggmtranstype);

            tobeInited_procs.push_back(oid);
            tobeInited_procs.push_back(agg->aggtransfn);
            tobeInited_procs.push_back(agg->aggfinalfn);
            tobeInited_procs.push_back(agg->aggcombinefn);
            tobeInited_procs.push_back(agg->aggserialfn);
            tobeInited_procs.push_back(agg->aggdeserialfn);
            tobeInited_procs.push_back(agg->aggmtransfn);
            tobeInited_procs.push_back(agg->aggminvtransfn);
            tobeInited_procs.push_back(agg->aggmfinalfn);

            tobeInited_opers.push_back(agg->aggsortop);
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return false;
    }
    
    for (const auto oid : tobeInited_types)
    {
        Typ::init(conn, oid);
    }
    for (const auto oid : tobeInited_procs)
    {
        Proc::init(conn, oid);
    }
    for (const auto oid : tobeInited_opers)
    {
        Oper::init(conn, oid);
    }

    return true;
    
};

}
