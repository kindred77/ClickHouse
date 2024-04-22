#include <Interpreters/orcaopt/metagen/Typ.h>
#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, TypPtr> Typ::typ_map;

bool Typ::init(PGConnectionPtr conn, PGOid oid)
{
    if (typ_map.count(oid) > 0) return true;
    try
    {
        if (!conn->is_open())
        {
            std::cout << "db not opened." << std::endl;
            return 1;
        }

        std::string sql = "select typname,typnamespace,typlen,typbyval,typtype,typcategory,"
                    "typispreferred,typeisdefined,typdelim,typrelid,typelem,typarray,typinput,typoutput,"
                    "typreceive,typsend,typmodin,typmodout,typanalyze,typalign,typstorage,typnotnull,"
                    "typbasetype,typtypmod,typndims,typcollation,lt_opr,eq_opr,gt_opr,hash_proc,cmp_proc "
                    "from pg_type where oid=$1";
        work worker(*conn.get());
        result resp = worker.exec_params(sql.c_str(), oid);
        for (auto i =0; i < resp.size(); ++i)
        {
            //std::cout << resp[i]["typname"] << "----" << resp[i]["typlen"] << "----" << typeid(resp[i][1]).name() << std::endl;
            auto typ = std::make_shared<Typ>();
            typ->oid = oid;
            typ->typname = resp[i]["typname"].as<std::string>();
            typ->typnamespace = resp[i]["typnamespace"].as<PGOid>();
            typ->typlen = resp[i]["typlen"].as<int>();
            typ->typbyval = resp[i]["typbyval"].as<bool>();
            typ->typtype = resp[i]["typtype"].as<std::string>();
            typ->typcategory = resp[i]["typcategory"].as<std::string>();
            typ->typispreferred = resp[i]["typispreferred"].as<bool>();
            typ->typeisdefined = resp[i]["typeisdefined"].as<bool>();
            typ->typdelim = resp[i]["typdelim"].as<std::string>();
            typ->typrelid = resp[i]["typrelid"].as<PGOid>();
            typ->typelem = resp[i]["typelem"].as<PGOid>();
            typ->typarray = resp[i]["typarray"].as<PGOid>();
            typ->typinput = resp[i]["typinput"].as<PGOid>();
            typ->typoutput = resp[i]["typoutput"].as<PGOid>();
            typ->typreceive = resp[i]["typreceive"].as<PGOid>();
            typ->typsend = resp[i]["typsend"].as<PGOid>();
            typ->typmodin = resp[i]["typmodin"].as<PGOid>();
            typ->typmodout = resp[i]["typmodout"].as<PGOid>();
            typ->typanalyze = resp[i]["typanalyze"].as<PGOid>();
            typ->typalign = resp[i]["typalign"].as<std::string>();
            typ->typstorage = resp[i]["typstorage"].as<std::string>();
            typ->typnotnull = resp[i]["typnotnull"].as<bool>();
            typ->typbasetype = resp[i]["typbasetype"].as<PGOid>();
            typ->typtypmod = resp[i]["typtypmod"].as<int>();
            typ->typndims = resp[i]["typndims"].as<int>();
            typ->typcollation = resp[i]["typcollation"].as<PGOid>();
            typ->lt_opr = resp[i]["lt_opr"].as<PGOid>();
            typ->eq_opr = resp[i]["eq_opr"].as<PGOid>();
            typ->gt_opr = resp[i]["gt_opr"].as<PGOid>();
            typ->hash_proc = resp[i]["hash_proc"].as<PGOid>();
            typ->cmp_proc = resp[i]["cmp_proc"].as<PGOid>();

            typ_map.insert({oid, typ});

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
