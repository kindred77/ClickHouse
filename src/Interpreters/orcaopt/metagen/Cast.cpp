#include <Interpreters/orcaopt/metagen/Cast.h>
#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Proc.h>

#include <Common/Exception.h>

#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<std::string, CastPtr> Cast::cast_map;

void Cast::initVarName(PGConnectionPtr conn, CastPtr cast)
{
    if (!Typ::init(conn, cast->castsource))
    {
        std::string msg = "Can not init var name of cast, can not get source type, cast oid: "
                        + std::to_string(cast->oid) + ", source type oid: " + std::to_string(cast->castsource);
        throw Exception(msg, 1);
    }
    const auto source_type = Typ::typ_map[cast->castsource];
    cast->var_name = source_type->typname;

    if (!Typ::init(conn, cast->casttarget))
    {
        std::string msg = "Can not init var name of cast, can not get target type, cast oid: "
                        + std::to_string(cast->oid) + ", target type oid: " + std::to_string(cast->casttarget);
        throw Exception(msg, 1);
    }
    const auto target_type = Typ::typ_map[cast->casttarget];
    cast->var_name += ("_TO_" + target_type->typname);

    boost::to_upper(cast->var_name);
}

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

        auto cast = std::make_shared<Cast>();
        
        {
            work worker(*conn.get());
            result resp = worker.exec_params(sql.c_str(), source_oid, target_oid);

            if (resp.size() > 1)
            {
                std::string msg = "Duplicated cast, source_oid: " + std::to_string(source_oid) + ", target_oid: " + std::to_string(target_oid);
                throw Exception(msg, 1);
            }
            else if(resp.size() == 0)
            {
                is_found = false;
                return is_found;
            }
            auto i = 0;

            cast->castsource = source_oid; //resp[i]["castsource"].as<PGOid>();
            cast->casttarget = target_oid; //resp[i]["casttarget"].as<PGOid>();
            cast->castfunc = resp[i]["castfunc"].as<PGOid>();
            cast->castcontext = resp[i]["castcontext"].as<std::string>();
            cast->castmethod = resp[i]["castmethod"].as<std::string>();
        }

        cast_map.insert({key, cast});

        tobeInited_types.push_back(cast->castsource);
        tobeInited_types.push_back(cast->casttarget);

        tobeInited_procs.push_back(cast->castfunc);

        is_found = true;

        initVarName(conn, cast);
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

void Cast::output()
{
    std::cout << "------------------Cast count: " << Cast::cast_map.size() << "------------------" << std::endl;
    for (const auto & [key, cast] : cast_map)
    {
        //NEW_CAST(INT64_TO_INT16, 20, 21, 714, 'a', 'f')
        std::cout << "NEW_CAST("
                  << cast->var_name << ", " << cast->castsource << ", " << cast->casttarget << ", " << cast->castfunc
                  << ", '" << cast->castcontext << "', '" << cast->castmethod << "'"
                  << ")" << std::endl;
    }
};

}
