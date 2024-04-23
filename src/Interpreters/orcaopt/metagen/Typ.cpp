#include <Interpreters/orcaopt/metagen/Typ.h>
#include <Interpreters/orcaopt/metagen/Proc.h>
#include <Interpreters/orcaopt/metagen/Oper.h>
#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

std::unordered_map<PGOid, TypPtr> Typ::typ_map;

bool Typ::init(PGConnectionPtr conn, PGOid oid)
{
    if (oid == InvalidOid) return false;
    if (typ_map.count(oid) > 0) return true;
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

        std::string sql = "select typname,typnamespace,typlen,typbyval,typtype,typcategory,"
                    "typispreferred,typeisdefined,typdelim,typrelid,typelem,typarray,typinput,typoutput,"
                    "typreceive,typsend,typmodin,typmodout,typanalyze,typalign,typstorage,typnotnull,"
                    "typbasetype,typtypmod,typndims,typcollation "
                    "from pg_type where oid=$1";
        
        std::string ext_sql = "select po.opcintype,pa.amopopr,"
                           "(select pop.oprname from pg_operator pop where pop.oid=pa.amopopr) as oprname,"
                           "case when po.opcmethod=403 then 'BTREE_AM_OID' else 'HASH_AM_OID' end as opcmethod,"
                           "case when po.opcmethod=405 and pa.amopstrategy=1 then 'HTEqualStrategyNumber' "
                           "when po.opcmethod=403 and pa.amopstrategy=1 then 'BTLessStrategyNumber' "
                           "when po.opcmethod=403 and pa.amopstrategy=2 then 'BTLessEqualStrategyNumber' "
                           "when po.opcmethod=403 and pa.amopstrategy=3 then 'BTEqualStrategyNumber' "
                           "when po.opcmethod=403 and pa.amopstrategy=4 then 'BTGreaterEqualStrategyNumber' "
                           "when po.opcmethod=403 and pa.amopstrategy=5 then 'BTGreaterStrategyNumber' "
                           "else 'Unknown: '||po.opcmethod||'-'||pa.amopstrategy "
                           "end as amopstrategy "
                           "from pg_amop pa "
                           "inner join pg_opclass po on pa.amopfamily = po.opcfamily "
                           "and pa.amoplefttype = po.opcintype and pa.amoprighttype = po.opcintype "
                           "and pa.amopstrategy in (1,2,3,4,5) "
                           "where po.opcmethod in (403, 405) "
                           "and po.opcdefault = true "
                           "and po.opcintype = $1";

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
            // typ->lt_opr = resp[i]["lt_opr"].as<PGOid>();
            // typ->eq_opr = resp[i]["eq_opr"].as<PGOid>();
            // typ->gt_opr = resp[i]["gt_opr"].as<PGOid>();
            // typ->hash_proc = resp[i]["hash_proc"].as<PGOid>();
            // typ->cmp_proc = resp[i]["cmp_proc"].as<PGOid>();

            if (typ->typrelid != InvalidOid)
            {
                std::string msg = "Type with relid is not supported yet! oid: " + std::to_string(oid) + ", name: " + typ->typname;
                throw msg;
            }

            //get ext info
            work worker_ext(*conn.get());
            result resp_ext = worker_ext.exec_params(ext_sql.c_str(), oid);
            for (auto j =0; j < resp_ext.size(); ++j)
            {
                auto opr = resp_ext[j]["amopopr"].as<PGOid>();
                auto strategy = resp_ext[j]["amopstrategy"].as<std::string>();
                if (strategy == "BTLessStrategyNumber")
                {
                    typ->lt_opr = opr;
                }
                else if(strategy == "BTEqualStrategyNumber")
                {
                    typ->eq_opr = opr;
                }
                else if(strategy == "BTGreaterStrategyNumber")
                {
                    typ->gt_opr = opr;
                }
                else if(strategy == "HTEqualStrategyNumber")
                {
                    typ->hash_proc = opr;
                }
            }

            typ->cmp_proc = InvalidOid;

            typ_map.insert({oid, typ});

            tobeInited_types.push_back(typ->typelem);
            tobeInited_types.push_back(typ->typarray);
            tobeInited_types.push_back(typ->typbasetype);

            tobeInited_opers.push_back(typ->lt_opr);
            tobeInited_opers.push_back(typ->eq_opr);
            tobeInited_opers.push_back(typ->gt_opr);

            tobeInited_procs.push_back(typ->typinput);
            tobeInited_procs.push_back(typ->typoutput);
            tobeInited_procs.push_back(typ->typreceive);
            tobeInited_procs.push_back(typ->typsend);
            tobeInited_procs.push_back(typ->typmodin);
            tobeInited_procs.push_back(typ->typmodout);
            tobeInited_procs.push_back(typ->typanalyze);
            tobeInited_procs.push_back(typ->hash_proc);
            tobeInited_procs.push_back(typ->cmp_proc);
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
