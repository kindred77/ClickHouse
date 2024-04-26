#include <Interpreters/orcaopt/metagen/Oper.h>
#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Proc.h>

#include <Common/Exception.h>

#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, OperPtr> Oper::oper_map;

void Oper::initVarName(PGConnectionPtr conn, OperPtr oper)
{
    if (!Proc::init(conn, oper->oprcode))
    {
        std::string msg = "Can not init var name of oper, can not get proc, oper oid: "
                        + std::to_string(oper->oid) + ", proc oid: " + std::to_string(oper->oprcode);
        throw Exception(msg, 1);
    }
    const auto proc = Proc::proc_map[oper->oprcode];
    oper->var_name = proc->proname;
    boost::to_upper(oper->var_name);
}

bool Oper::init(PGConnectionPtr conn, PGOid oid)
{
    if (oid == InvalidOid) return false;
    if (oper_map.count(oid) > 0) return true;
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

        std::string sql = "select oprname,oprnamespace,oprowner,oprkind,oprcanmerge,oprcanhash,oprleft,oprright,oprresult,oprcom,oprnegate,"
                    "oid(oprcode) as oprcode,oid(oprrest) as oprrest,oid(oprjoin) as oprjoin from pg_operator where oid=$1";
        auto oper = std::make_shared<Oper>();
        {
            work worker(*conn.get());
            result resp = worker.exec_params(sql.c_str(), oid);

            if (resp.size() > 1)
            {
                std::string msg = "Duplicated type, oid: " + std::to_string(oid);
                throw Exception(msg, 1);
            }
            else if(resp.size() == 0)
            {
                is_found = false;
                return is_found;
            }
            auto i = 0;

            oper->oid = oid;
            oper->oprname = resp[i]["oprname"].as<std::string>();
            oper->oprnamespace = resp[i]["oprnamespace"].as<PGOid>();
            oper->oprowner = resp[i]["oprowner"].as<PGOid>();
            oper->oprkind = resp[i]["oprkind"].as<std::string>();
            oper->oprcanmerge = resp[i]["oprcanmerge"].as<bool>();
            oper->oprcanhash = resp[i]["oprcanhash"].as<bool>();
            oper->oprleft = resp[i]["oprleft"].as<PGOid>();
            oper->oprright = resp[i]["oprright"].as<PGOid>();
            oper->oprresult = resp[i]["oprresult"].as<PGOid>();
            oper->oprcom = resp[i]["oprcom"].as<PGOid>();
            oper->oprnegate = resp[i]["oprnegate"].as<PGOid>();
            oper->oprcode = resp[i]["oprcode"].as<PGOid>();
            oper->oprrest = resp[i]["oprrest"].as<PGOid>();
            oper->oprjoin = resp[i]["oprjoin"].as<PGOid>();
        }

        oper_map.insert({oid, oper});

        tobeInited_types.push_back(oper->oprleft);
        tobeInited_types.push_back(oper->oprright);
        tobeInited_types.push_back(oper->oprresult);

        tobeInited_opers.push_back(oper->oprcom);
        tobeInited_opers.push_back(oper->oprnegate);

        tobeInited_procs.push_back(oper->oprcode);
        tobeInited_procs.push_back(oper->oprrest);
        tobeInited_procs.push_back(oper->oprjoin);

        is_found = true;

        initVarName(conn, oper);
    }
    catch(const std::exception& e)
    {
        std::cerr << "Init oper " << oid << " failed: " << e.what() << '\n';
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

void Oper::output()
{
    std::cout << "------------------Oper count: " << Oper::oper_map.size() << "------------------" << std::endl;
    for (const auto & [key, oper] : oper_map)
    {
        //NEW_OPER(INT2PL, 550, "+", 1, 1, 'b', false, false, 21, 21, 21, 550, 0, 0, 0, 0, 0, 176, 0, 0)
        std::cout << "NEW_OPER("
                  << oper->var_name << ", " << oper->oid << ", \"" << oper->oprname << "\", " << oper->oprnamespace
                  << ", " << oper->oprowner << ", '" << oper->oprkind << "', " << (oper->oprcanmerge ? "true" : "false")
                  << ", " << (oper->oprcanhash ? "true" : "false") << ", " << oper->oprleft << ", " << oper->oprright
                  << ", " << oper->oprresult << ", " << oper->oprcom << ", " << oper->oprnegate << ", " << oper->oprcode
                  << ", " << oper->oprrest << ", " << oper->oprjoin
                  << ")" << std::endl;
    }

    std::cout << "------------------------------------" << std::endl;
    for (const auto & [key, oper] : oper_map)
    {
        std::cout << oper->var_name << std::endl;
    }
};

}
