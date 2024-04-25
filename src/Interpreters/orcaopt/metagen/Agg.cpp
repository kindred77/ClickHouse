#include <Interpreters/orcaopt/metagen/Agg.h>
#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Proc.h>
#include <Interpreters/orcaopt/metagen/Oper.h>

#include <Common/Exception.h>

#include <iostream>

#include <boost/algorithm/string/case_conv.hpp>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, AggPtr> Agg::agg_map;

void Agg::initVarName(PGConnectionPtr conn, AggPtr agg)
{
    if (!Proc::init(conn, agg->aggfnoid))
    {
        std::string msg = "Can not init var name of agg, can not get proc, agg oid: " + std::to_string(agg->aggfnoid);
        throw Exception(msg, 1);
    }
    const auto proc = Proc::proc_map[agg->aggfnoid];
    if (proc->proargtypes.size() > 1)
    {
        std::string msg = "Can not init var name of agg, multiple args, agg oid: " + std::to_string(agg->aggfnoid);
        throw Exception(msg, 1);
    }

    agg->var_name = proc->proname;

    if(proc->proargtypes.size() == 1 && proc->proargtypes[0] != InvalidOid)
    {
        auto typ_oid = proc->proargtypes[0];

        if (!Typ::init(conn, typ_oid))
        {
            std::string msg = "Can not init var name of agg, can not get arg type, agg oid: " + std::to_string(agg->aggfnoid) + ", type oid: " + std::to_string(typ_oid);
            throw Exception(msg, 1);
        }

        auto typ = Typ::typ_map[typ_oid];

        agg->var_name += "_";
        agg->var_name += typ->typname;
    }

    boost::to_upper(agg->var_name);
    
}

bool Agg::init(PGConnectionPtr conn, PGOid oid)
{
    if (oid == InvalidOid) return false;
    if (agg_map.count(oid) > 0) return true;
    bool is_found = false;
    std::vector<PGOid> tobeInited_types;
    std::vector<PGOid> tobeInited_procs;
    std::vector<PGOid> tobeInited_opers;
    try
    {
        if (!conn->is_open())
        {
            throw Exception("DB not opened! ", 1);
        }

        std::string sql = "select aggfnoid,aggkind,aggnumdirectargs,oid(aggtransfn) as aggtransfn,"
                    "oid(aggfinalfn) as aggfinalfn,oid(aggcombinefn) as aggcombinefn,"
                    "oid(aggserialfn) as aggserialfn,oid(aggdeserialfn) as aggdeserialfn,"
                    "oid(aggmtransfn) as aggmtransfn,oid(aggminvtransfn) as aggminvtransfn,"
                    "oid(aggmfinalfn) as aggmfinalfn,aggfinalextra,aggmfinalextra,aggsortop,"
                    "aggtranstype,aggtransspace,aggmtranstype,aggmtransspace,agginitval,aggminitval from pg_aggregate where aggfnoid=$1";

        auto agg = std::make_shared<Agg>();

        {
            work worker(*conn.get());
            result resp = worker.exec_params(sql.c_str(), oid);

            if (resp.size() > 1)
            {
                std::string msg = "Duplicated agg, oid: " + std::to_string(oid);
                throw Exception(msg, 1);
            }
            else if(resp.size() == 0)
            {
                is_found = false;
                return is_found;
            }
            auto i = 0;

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
        }

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

        is_found = true;

        initVarName(conn, agg);
    }
    catch(const Exception& e)
    {
        std::cerr << "Init agg " << oid << " failed: " << e.what() << '\n';
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
    
    return is_found;
    
};

void Agg::output()
{
    std::cout << "------------------Agg count: " << Agg::agg_map.size() << "------------------" << std::endl;
    for (const auto & [key, agg] : agg_map)
    {
        //NEW_AGG(COUNTANY, 2147, 'n', 0, 2804, InvalidOid, 463, InvalidOid, InvalidOid, 2804, 3547, InvalidOid, false, false, InvalidOid, 20, InvalidOid, 20, InvalidOid)
        std::cout << "NEW_AGG("
                  << agg->var_name << ", " << agg->aggfnoid << ", '" << agg->aggkind << "', " << agg->aggnumdirectargs
                  << ", " << agg->aggtransfn << ", " << agg->aggfinalfn << ", " << agg->aggcombinefn << ", " << agg->aggserialfn
                  << ", " << agg->aggdeserialfn << ", " << agg->aggmtransfn << ", " << agg->aggminvtransfn << ", " << agg->aggmfinalfn
                  << ", " << (agg->aggfinalextra ? "true" : "false") << ", " << (agg->aggmfinalextra ? "true" : "false") << ", " << agg->aggsortop << ", " << agg->aggtranstype
                  << ", " << agg->aggtransspace << ", " << agg->aggmtranstype << ", " << agg->aggmtransspace
                  //<< ", " << agg->agginitval << ", " << agg->aggminitval
                  << ")" << std::endl;
    }
}

}
