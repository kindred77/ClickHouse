#include "GRPCForQueryPlan.h"

#include <common/range.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/SelectQueryInfo.h>
#include "clickhouse_grpc.grpc.pb.h"
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
}

void GRPCDoTableScan([[maybe_unused]] ContextMutablePtr& query_context, [[maybe_unused]] QueryPlan & query_plan, [[maybe_unused]] GRPCTableScanStep table_scan_step)
{
    const Settings & settings = query_context->getSettingsRef();

    if (!DatabaseCatalog::instance().isDatabaseExist(table_scan_step.db_name()))
        throw Exception("Database " + table_scan_step.db_name() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

    SelectQueryInfo query_info;
    StorageID table_id(table_scan_step.db_name(), table_scan_step.table_name());

    if (!DatabaseCatalog::instance().isTableExist(table_id, query_context))
        throw Exception("Table " + table_scan_step.table_name() + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

    StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, query_context);
    StorageMetadataPtr meta_data = storage->getInMemoryMetadataPtr();
    Names required_columns(table_scan_step.required_columns().begin(), table_scan_step.required_columns().end());
    storage->read(query_plan, required_columns, meta_data, query_info, query_context, QueryProcessingStage::FetchColumns, settings.max_block_size, 8);
}

void GRPCDoFilter([[maybe_unused]] ContextMutablePtr& query_context, [[maybe_unused]] QueryPlan & query_plan, [[maybe_unused]] GRPCFilterStep filter_step)
{

}

Block CreateTestBlock()
{
    Block test_block = {ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "$1"),
            ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "a"),
            ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "b")};
    MutableColumns columns = test_block.mutateColumns();
    columns[0]->insert("a1");
    columns[0]->insert("a2");
    columns[1]->insert("b1");
    columns[1]->insert("b2");
    columns[2]->insert("c1");
    columns[2]->insert("c2");

    test_block.setColumns(std::move(columns));

    return test_block;
}

void DumpBlock(Poco::Logger * log, Block block)
{
    for(auto col : block)
    {
        LOG_INFO(log, "column name------------{}------------start", col.name);
        if(col.column == nullptr)
        {
            LOG_INFO(log, "------col.column is null");
        }
        else
        {
            LOG_INFO(log, "------col.column is not null-----size: {}", col.column->size());
        }
        if(col.column && col.column->size() > 0)
        {
            LOG_INFO(log, "size----{}", col.column->size());
            for(auto i : collections::range(col.column->size()))
            {
                LOG_INFO(log, "i----{}", i);
                LOG_INFO(log, "data----{}", col.column->getDataAt(i));
            }
        }

        LOG_INFO(log, "column name------------{}------------end", col.name);
    }
}

void TestExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log, std::string query_text)
{
    const Settings & settings = query_context->getSettingsRef();

    auto test_block = std::move(CreateTestBlock());

    const char * begin = query_text.data();
    const char * end = begin + query_text.size();
    ParserExpressionWithOptionalAlias parser(false);
    ASTPtr filter_ast = parseQuery(parser, begin, end, "", settings.max_query_size, settings.max_parser_depth);

//  auto actions = std::make_shared<ActionsDAG>(test_block.getColumnsWithTypeAndName());
//  PreparedSets prepared_sets;
//  SubqueriesForSets subquery_for_sets;
//  ActionsVisitor::Data visitor_data(*query_context, SizeLimits{}, 1, {}, std::move(actions), prepared_sets, subquery_for_sets, true, true, true, false);
//  ActionsVisitor(visitor_data).visit(filter_ast);
//  actions = visitor_data.getActions();
//  auto expression_actions = std::make_shared<ExpressionActions>(actions);

    SettingsChanges settings_changes;
    settings_changes.push_back({"max_ast_depth", 1000});
    query_context->checkSettingsConstraints(settings_changes);
    query_context->applySettingsChanges(settings_changes);

    auto syntax_reulst = TreeRewriter(query_context).analyze(filter_ast, test_block.getNamesAndTypesList());
    ExpressionAnalyzer analyzer(filter_ast, syntax_reulst, query_context);
    auto expression_actions = analyzer.getActions(false, true);


    LOG_INFO(log, "------------------expression_actions:{}---", expression_actions->dumpActions());

    LOG_INFO(log, "------------before execute---------------");
    DumpBlock(log, test_block);

    expression_actions->execute(test_block);

    LOG_INFO(log, "------------after execute---------------");
    DumpBlock(log, test_block);
}

void TestConstExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log)
{
    const Settings & settings = query_context->getSettingsRef();

    auto test_block = std::move(CreateTestBlock());

    std::string sql_test = "select a,b,1+2 from test";
    const char * begin_test = sql_test.data();
    const char * end_test = begin_test + sql_test.size();
    ParserQuery parser_test(end_test);
    ASTPtr query_ast_test = parseQuery(parser_test, begin_test, end_test, "", settings.max_query_size, settings.max_parser_depth);
    Block result_test {
            {DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy"}
    };
    auto syntax_reulst_test = TreeRewriter(query_context).analyze(query_ast_test, test_block.getNamesAndTypesList());
    ExpressionAnalyzer analyzer_test(query_ast_test, syntax_reulst_test, query_context);
    auto expression_actions_test = analyzer_test.getConstActions();
    expression_actions_test->execute(result_test);
    DumpBlock(log, result_test);
}

}

