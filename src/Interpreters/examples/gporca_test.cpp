#include <iostream>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/Context.h>
#include <postgres_parser.hpp>

int main(int argc, char ** argv)
{
    std::string query_str = "select 1";
    std::cout << "000000000000" << std::endl;
    duckdb::PostgresParser::SetPreserveIdentifierCase(true);
    std::cout << "1111111111111" << std::endl;
    duckdb::PostgresParser parser;
    parser.Parse(query_str);
    std::cout << "2222222222222" << std::endl;
    if (!parser.success || !parser.parse_tree)
    {
        std::cout << "Failed!" << std::endl;
        return -1;
    }
    std::cout << "3333333333333333" << std::endl;
    auto shared_context = DB::Context::createShared();
    auto global_context = DB::Context::createGlobal(shared_context.get());
    std::cout << "4444444444444" << std::endl;
    for (auto entry = parser.parse_tree->head; entry != nullptr; entry = entry->next)
    {
        auto query_node = (duckdb_libpgquery::PGNode *)entry->data.ptr_value;
        std::cout << "5555555555555" << std::endl;
        auto select_parser = std::make_shared<DB::SelectParser>(global_context);
        std::cout << "666666666666" << std::endl;
        auto ps_stat = std::make_shared<duckdb_libpgquery::PGParseState>();
        std::cout << "77777777777" << std::endl;
        auto query = select_parser->transformStmt(ps_stat.get(), query_node);
        std::cout << "88888888888888888" << std::endl;
    }
    return 0;
}
