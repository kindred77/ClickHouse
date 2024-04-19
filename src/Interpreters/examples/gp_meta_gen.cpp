#include <pqxx/pqxx>
#include <string>
#include <iostream>

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/metagen/common.h>
#include <Interpreters/orcaopt/metagen/Agg.h>
#include <Interpreters/orcaopt/metagen/Proc.h>

using namespace pqxx;
using namespace duckdb_libpgquery;

int main(int argc, char ** argv)
{
    PGConnectionPtr conn = std::make_shared<connection>("dbname=postgres user=kindred hostaddr=127.0.0.1 port=5432");
    
    return 0;
};
