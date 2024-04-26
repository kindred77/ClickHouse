#include <Interpreters/orcaopt/metagen/Proc.h>
#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Agg.h>

#include <Common/Exception.h>

#include <iostream>
#include <boost/algorithm/string.hpp>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, ProcPtr> Proc::proc_map;

void Proc::initVarName(PGConnectionPtr conn, ProcPtr proc)
{
    proc->var_name = proc->proname;
    boost::to_upper(proc->var_name);
}

bool Proc::init(PGConnectionPtr conn, PGOid oid)
{
    if (oid == InvalidOid) return false;
    if (proc_map.count(oid) > 0) return true;
    bool is_found = false;
    std::vector<PGOid> tobeInited_types;
    std::vector<PGOid> tobeInited_procs;
    std::vector<PGOid> tobeInited_aggs;
    try
    {
        if (!conn->is_open())
        {
            std::cout << "db not opened." << std::endl;
            return 1;
        }

        std::string sql = "select oid,proname,pronamespace,proowner,prolang,procost,prorows,provariadic,"
                    "oid(protransform) as protransform,proisagg,proiswindow,"
                    "prosecdef,proleakproof,proisstrict,proretset,provolatile,pronargs,pronargdefaults,"
                    "prorettype,proargtypes from pg_proc where oid=$1";
        
        auto proc = std::make_shared<Proc>();
        
        {
            work worker(*conn.get());
            result resp = worker.exec_params(sql.c_str(), oid);

            if (resp.size() > 1)
            {
                std::string msg = "Duplicated proc, oid: " + std::to_string(oid);
                throw Exception(msg, 1);
            }
            else if(resp.size() == 0)
            {
                is_found = false;
                return is_found;
            }
            auto i = 0;

            proc->oid = oid;
            proc->proname = resp[i]["proname"].as<std::string>();
            proc->pronamespace = resp[i]["pronamespace"].as<PGOid>();
            proc->proowner = resp[i]["proowner"].as<PGOid>();
            proc->prolang = resp[i]["prolang"].as<PGOid>();
            proc->procost = resp[i]["procost"].as<float>();
            proc->prorows = resp[i]["prorows"].as<float>();
            proc->provariadic = resp[i]["provariadic"].as<PGOid>();
            proc->protransform = resp[i]["protransform"].as<PGOid>();
            proc->proisagg = resp[i]["proisagg"].as<bool>();
            proc->proiswindow = resp[i]["proiswindow"].as<bool>();
            proc->prosecdef = resp[i]["prosecdef"].as<bool>();
            proc->proleakproof = resp[i]["proleakproof"].as<bool>();
            proc->proisstrict = resp[i]["proisstrict"].as<bool>();
            proc->proretset = resp[i]["proretset"].as<bool>();
            proc->provolatile = resp[i]["provolatile"].as<std::string>();
            proc->pronargs = resp[i]["pronargs"].as<int>();
            proc->pronargdefaults = resp[i]["pronargdefaults"].as<int>();
            proc->prorettype = resp[i]["prorettype"].as<PGOid>();

            auto str = resp[i]["proargtypes"].as<std::string>();
            std::vector<std::string> str_oids;
            boost::split(str_oids, str, [](char c) { return c == ' '; });
            for (auto str_oid : str_oids)
            {
                proc->proargtypes.push_back(std::atoi(str_oid.c_str()));
            }
        }
            
        proc_map.insert({oid, proc});

        tobeInited_types.push_back(proc->provariadic);
        tobeInited_types.push_back(proc->prorettype);
        tobeInited_types.insert(tobeInited_types.end(), proc->proargtypes.begin(), proc->proargtypes.end());

        tobeInited_procs.push_back(proc->protransform);

        if (proc->proisagg)
        {
            tobeInited_aggs.push_back(oid);
        }

        is_found = true;

        initVarName(conn, proc);
    }
    catch(const std::exception& e)
    {
        std::cerr << "Init proc " << oid << " failed: " << e.what() << '\n';
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
    for (const auto oid : tobeInited_aggs)
    {
        Agg::init(conn, oid);
    }
    return is_found;
    
};

void Proc::output()
{
    std::cout << "------------------Proc count: " << Proc::proc_map.size() << "------------------" << std::endl;
    for (const auto & [key, proc] : proc_map)
    {
        //NEW_PROC(INT2PL, 176, "plus", 1, 1, 12, 1, 0.0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 21, {PGOid(21), PGOid(21)})
        std::string proargtypes_str;
        if (proc->proargtypes.size() > 0)
        {
            proargtypes_str = "{";
        }
        for (size_t i = 0; i < proc->proargtypes.size(); ++i)
        {
            auto typ_oid = proc->proargtypes[i];
            proargtypes_str += ("PGOid(" + std::to_string(typ_oid) + ")");
            if (i < proc->proargtypes.size() - 1)
            {
                proargtypes_str += ", ";
            }
        }
        if (proc->proargtypes.size() > 0)
        {
            proargtypes_str += "}";
        }

        std::cout << "NEW_PROC("
                  << proc->var_name << ", " << proc->oid << ", \"" << proc->proname << "\", " << proc->pronamespace
                  << ", " << proc->proowner << ", " << proc->prolang << ", " << proc->procost << ", " << proc->prorows
                  << ", " << proc->provariadic << ", " << proc->protransform << ", " << (proc->proisagg ? "true" : "false")
                  << ", " << (proc->proiswindow ? "true" : "false") << ", " << (proc->prosecdef ? "true" : "false")
                  << ", " << (proc->proleakproof ? "true" : "false") << ", " << (proc->proisstrict ? "true" : "false")
                  << ", " << (proc->proretset ? "true" : "false") << ", '" << proc->provolatile << "', " << proc->pronargs
                  << ", " << proc->pronargdefaults << ", " << proc->prorettype << ", " << proargtypes_str
                  << ")" << std::endl;
    }

    std::cout << "------------------------------------" << std::endl;
    for (const auto & [key, proc] : proc_map)
    {
        std::cout << proc->var_name << std::endl;
    }
};

}
