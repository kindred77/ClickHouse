#include <Interpreters/orcaopt/metagen/Oper.h>
#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, OperPtr> Oper::oper_map;

bool Oper::init(PGConnectionPtr conn, PGOid oid)
{
    if (oper_map.count(oid) > 0) return true;
    try
    {
        if (!conn->is_open())
        {
            std::cout << "db not opened." << std::endl;
            return 1;
        }

        std::string sql = "select oprname,oprnamespace,oprowner,oprkind,oprcanmerge,oprcanhash,oprleft,oprright,oprresult,oprcom,oprnegate,"
                    "oprcode,oprrest,oprjoin from pg_operator where oid=$1";
        work worker(*conn.get());
        result resp = worker.exec_params(sql.c_str(), oid);
        for (auto i =0; i < resp.size(); ++i)
        {
            //std::cout << resp[i]["typname"] << "----" << resp[i]["typlen"] << "----" << typeid(resp[i][1]).name() << std::endl;
            auto oper = std::make_shared<Oper>();
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

            oper_map.insert({oid, oper});

            return true;
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    return false;
    
};

}
