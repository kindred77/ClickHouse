#include <Interpreters/orcaopt/metagen/Cast.h>
#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Proc.h>
#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<std::string, CastPtr> Cast::cast_map;

bool Cast::init(PGConnectionPtr conn, PGOid source_oid, PGOid target_oid)
{
    if (source_oid == InvalidOid || target_oid == InvalidOid) return false;
    std::string key = std::to_string(source_oid) + "_" + std::to_string(target_oid);
    if (cast_map.count(key) > 0) return true;
    bool is_found = false;
    std::vector<PGOid> tobeInited_types;
    std::vector<PGOid> tobeInited_procs;
    try
    {
        if (!conn->is_open())
        {
            std::cout << "db not opened." << std::endl;
            return 1;
        }

        std::string sql = "select castsource,casttarget,castfunc,castcontext,castmethod "
                    "from pg_cast where castsource=$1 and casttarget=$2";

        work worker(*conn.get());
        result resp = worker.exec_params(sql.c_str(), source_oid, target_oid);
        for (auto i =0; i < resp.size(); ++i)
        {
            //std::cout << resp[i]["typname"] << "----" << resp[i]["typlen"] << "----" << typeid(resp[i][1]).name() << std::endl;
            auto cast = std::make_shared<Cast>();
            cast->castsource = source_oid; //resp[i]["castsource"].as<PGOid>();
            cast->casttarget = target_oid; //resp[i]["casttarget"].as<PGOid>();
            cast->castfunc = resp[i]["castfunc"].as<PGOid>();
            cast->castcontext = resp[i]["castcontext"].as<std::string>();
            cast->castmethod = resp[i]["castmethod"].as<std::string>();

            cast_map.insert({key, cast});

            tobeInited_types.push_back(cast->castsource);
            tobeInited_types.push_back(cast->casttarget);

            tobeInited_procs.push_back(cast->castfunc);

            is_found = true;
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << "Init cast (" << source_oid << ", " << target_oid << ") failed: " << e.what() << '\n';
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

    return is_found;
    
};

}
