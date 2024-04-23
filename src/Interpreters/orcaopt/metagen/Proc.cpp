#include <Interpreters/orcaopt/metagen/Proc.h>
#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Agg.h>
#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, ProcPtr> Proc::proc_map;

bool Proc::init(PGConnectionPtr conn, PGOid oid)
{
    if (oid == InvalidOid) return false;
    if (proc_map.count(oid) > 0) return true;
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

        std::string sql = "select oid,proname,pronamespace,proowner,prolang,procost,prorows,provariadic,protransform,proisagg,proiswindow,"
                    "prosecdef,proleakproof,proisstrict,proretset,provolatile,pronargs,pronargdefaults,prorettype,proargtypes from pg_proc where oid=$1";
        work worker(*conn.get());
        result resp = worker.exec_params(sql.c_str(), oid);
        for (auto i =0; i < resp.size(); ++i)
        {
            //std::cout << resp[i]["typname"] << "----" << resp[i]["typlen"] << "----" << typeid(resp[i][1]).name() << std::endl;
            auto proc = std::make_shared<Proc>();
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
            //TODO
            std::cout << "proargtypes:----" << resp[i]["proargtypes"].as<std::string>();
            
            proc_map.insert({oid, proc});

            tobeInited_types.push_back(proc->provariadic);
            tobeInited_types.push_back(proc->prorettype);
            //TODO
            //proc.proargtypes=new ArrayList<Oid>(arr.);
            
            tobeInited_procs.push_back(proc->protransform);

            if (proc->proisagg)
            {
                tobeInited_aggs.push_back(oid);
            }
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
    for (const auto oid : tobeInited_aggs)
    {
        Agg::init(conn, oid);
    }
    return true;
    
};

}
