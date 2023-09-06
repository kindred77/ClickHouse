#include <iostream>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/Context.h>
#include <postgres_parser.hpp>

int main(int argc, char ** argv)
{
    std::string query_str = "select 1";
    duckdb::PostgresParser::SetPreserveIdentifierCase(true);
    duckdb::PostgresParser parser;
    parser.Parse(query_str);
    if (!parser.success || !parser.parse_tree)
    {
        std::cout << "Failed!" << std::endl;
        return -1;
    }
    auto shared_context = DB::Context::createShared();
    auto global_context = DB::Context::createGlobal(shared_context.get());
    for (auto entry = parser.parse_tree->head; entry != nullptr; entry = entry->next)
    {
        auto query_node = (duckdb_libpgquery::PGNode *)entry->data.ptr_value;
        auto select_parser = std::make_shared<DB::SelectParser>(global_context);
        auto ps_stat = std::make_shared<duckdb_libpgquery::PGParseState>();
        auto query = select_parser->transformStmt(ps_stat.get(), query_node);
    }
    return 0;
}
