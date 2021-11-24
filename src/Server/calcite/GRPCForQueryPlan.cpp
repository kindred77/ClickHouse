#include "GRPCForQueryPlan.h"

#include <common/range.h>
#include <Common/typeid_cast.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
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

void doTableScanForGRPC([[maybe_unused]] ContextMutablePtr& query_context, [[maybe_unused]] QueryPlan & query_plan, [[maybe_unused]] GRPCTableScanStep table_scan_step)
{
    Poco::Logger * log = &Poco::Logger::get("GRPCForQueryPlan");
    const Settings & settings = query_context->getSettingsRef();



    //if (!DatabaseCatalog::instance().isDatabaseExist(table_scan_step.db_name()))
        //throw Exception("Database " + table_scan_step.db_name() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

    LOG_INFO(log, "-----------000000-------");
    const char * begin_filter = table_scan_step.filter().data();
    const char * end_filter = begin_filter + table_scan_step.filter().size();
    ParserExpressionWithOptionalAlias parser_filter(false);
    LOG_INFO(log, "-----------111111-------");
    auto select_ast_ptr = std::make_shared<ASTSelectQuery>();
    select_ast_ptr->setExpression(ASTSelectQuery::Expression::WHERE, parseQuery(parser_filter, begin_filter, end_filter, "", settings.max_query_size, settings.max_parser_depth));

    const char * begin_proj = table_scan_step.projection().data();
    const char * end_proj = begin_proj + table_scan_step.projection().size();
    ParserNotEmptyExpressionList parser_proj(true);
    select_ast_ptr->setExpression(ASTSelectQuery::Expression::SELECT, parseQuery(parser_proj, begin_proj, end_proj, "", settings.max_query_size, settings.max_parser_depth));
    LOG_INFO(log, "-----------222222-------");

    const char * begin_tables = table_scan_step.table_name().data();
    const char * end_tables = begin_tables + table_scan_step.table_name().size();
    ParserTablesInSelectQuery parser_tables;
    select_ast_ptr->setExpression(ASTSelectQuery::Expression::TABLES, parseQuery(parser_tables, begin_tables, end_tables, "", settings.max_query_size, settings.max_parser_depth));
    //OG_INFO(log, "-----------222222aaaa-------{}", query_ast_ptr->getExpression(ASTSelectQuery::Expression::TABLES));

    auto tables_in_select_query = select_ast_ptr->tables()->as<ASTTablesInSelectQuery>();
    StorageID table_id = StorageID::createEmpty();
    if(!tables_in_select_query->children.empty())
    {
        auto tables_element = tables_in_select_query->children[0]->as<ASTTablesInSelectQueryElement>();
        if(tables_element->table_expression)
        {
            auto table_expression = tables_element->table_expression->as<ASTTableExpression>();
            if(table_expression && table_expression->database_and_table_name)
            {
                auto db_and_table_name = table_expression->database_and_table_name->as<ASTTableIdentifier>();
                if(db_and_table_name)
                {
                    table_id = StorageID(*db_and_table_name);
                    LOG_INFO(log, "-----------222222bbb-------{}----{}", db_and_table_name->getTableId().database_name, db_and_table_name->getTableId().table_name);
                }
            }
        }
    }

    if(table_id.empty())
    {
        throw Exception("Can not get table name.", ErrorCodes::UNKNOWN_TABLE);
    }

    if (!DatabaseCatalog::instance().isDatabaseExist(table_id.getDatabaseName()))
        throw Exception("Database " + table_id.getDatabaseName() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

    WriteBufferFromOwnString write_buf;
    formatAST(*select_ast_ptr, write_buf, false, false);
    //query_ast_ptr->format(FormatSettings(write_buf, true));
    LOG_INFO(log, "-----------222222ccc-------{}", write_buf.str());

    SelectQueryInfo query_info;
    query_info.query = select_ast_ptr;
    //auto analyzer_result = TreeRewriterResult{.required_source_columns={}};
    //query_info.syntax_analyzer_result = std::make_shared<const TreeRewriterResult>(analyzer_result);
    //StorageID table_id(table_scan_step.db_name(), table_scan_step.table_name());

    if (!DatabaseCatalog::instance().isTableExist(table_id, query_context))
        throw Exception("Table " + table_id.getTableName() + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);
    LOG_INFO(log, "-----------333333-------");
    //StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, query_context);
    //StorageMetadataPtr meta_data = storage->getInMemoryMetadataPtr();
    LOG_INFO(log, "-----------444444-------");
    //Names required_columns(table_scan_step.required_columns().begin(), table_scan_step.required_columns().end());
    //storage->read(query_plan, required_columns, meta_data, query_info, query_context, QueryProcessingStage::FetchColumns, settings.max_block_size, 8);
    //io = ::DB::executeQuery(write_buf.str(), query_context, false, QueryProcessingStage::Complete, true, true);
    ASTPtr query_ast = std::move(select_ast_ptr);
    auto interpreter = InterpreterFactory::get(query_ast, query_context, SelectQueryOptions(QueryProcessingStage::Complete));
    if(auto * select_interpreter = typeid_cast<InterpreterSelectQuery *>(&*interpreter))
    {
        select_interpreter->buildQueryPlan(query_plan);
    }

    LOG_INFO(log, "-----------555555-------");
}

void doFilterForGRPC([[maybe_unused]] ContextMutablePtr& query_context, [[maybe_unused]] QueryPlan & query_plan, [[maybe_unused]] GRPCFilterStep filter_step)
{

}

Block createTestBlock()
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

void dumpBlock(Poco::Logger * log, Block block)
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

void testExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log, std::string query_text)
{
    const Settings & settings = query_context->getSettingsRef();

    auto test_block = std::move(createTestBlock());

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
    dumpBlock(log, test_block);

    expression_actions->execute(test_block);

    LOG_INFO(log, "------------after execute---------------");
    dumpBlock(log, test_block);
}

void testConstExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log)
{
    const Settings & settings = query_context->getSettingsRef();

    auto test_block = std::move(createTestBlock());

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
    dumpBlock(log, result_test);
}

}

