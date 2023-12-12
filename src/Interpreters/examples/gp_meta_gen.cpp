#include <pqxx/pqxx>
#include <string>
#include <iostream>

using namespace pqxx;

int main()
{
    try
    {
        connection gp("dbname=postgres user=kindred hostaddr=127.0.0.1 port=5432");
        if (!gp.is_open())
        {
            std::cout << "db not opened." << std::endl;
            return 1;
        }

        std::string sql = "select typname,typlen from pg_type where oid=16";
        work worker(gp);
        result resp = worker.exec(sql.c_str());
        for (auto i =0; i < resp.size(); ++i)
        {
            std::cout << resp[i]["typname"] << "----" << resp[i]["typlen"] << "----" << typeid(resp[i][1]).name() << std::endl;
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return 1;
    }
    
    return 0;
};