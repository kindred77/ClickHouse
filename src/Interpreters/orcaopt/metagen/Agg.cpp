#include <Interpreters/orcaopt/metagen/Agg.h>
#include <iostream>

using namespace duckdb_libpgquery;
using namespace pqxx;

namespace DB
{

bool Agg::init(PGConnectionPtr conn, PGOid oid)
{
    try
    {
        if (!conn->is_open())
        {
            std::cout << "db not opened." << std::endl;
            return 1;
        }

        std::string sql = "select typname,typlen from pg_type where oid=16";
        work worker(*conn.get());
        result resp = worker.exec(sql.c_str());
        for (auto i =0; i < resp.size(); ++i)
        {
            std::cout << resp[i]["typname"] << "----" << resp[i]["typlen"] << "----" << typeid(resp[i][1]).name() << std::endl;
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
    
};

}
