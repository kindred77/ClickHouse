#include "GRPCForQueryPlan.h"

#include <common/range.h>
#include <Common/typeid_cast.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Processors/QueryPlan/ForCalcite/TableScan.h>
#include <Interpreters/orcaopt/TranslatorQueryToDXL.h>
#include "clickhouse_grpc.grpc.pb.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int INDEX_NOT_USED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

static TableScan::AnalysisResult selectRangesToRead(
        const StorageMergeTree & storage,
        ContextPtr context,
        Names real_column_names,
        const size_t requested_num_streams,
        SelectQueryInfo query_info,
        Poco::Logger * log,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr,
        const bool sample_factor_column_queried = false)
{
    TableScan::AnalysisResult result;
    const auto & settings = context->getSettingsRef();
    StorageMetadataPtr metadata_snapshot = storage.getInMemoryMetadataPtr();
    MergeTreeData::DataPartsVector parts = storage.getDataPartsVector();
    MergeTreeReaderSettings reader_settings = getMergeTreeReaderSettings(context);

    size_t total_parts = parts.size();

    auto part_values = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(storage, parts, query_info.query, context);
    if (part_values && part_values->empty())
        return result;

    result.column_names_to_read = real_column_names;

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (result.column_names_to_read.empty())
    {
        NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
        result.column_names_to_read.push_back(ExpressionActions::getSmallestColumn(available_real_columns));
    }

    metadata_snapshot->check(result.column_names_to_read, storage.getVirtuals(), storage.getStorageID());

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    Names primary_key_columns = primary_key.column_names;
    KeyCondition key_condition(query_info, context, primary_key_columns, primary_key.expression);

    if (settings.force_primary_key && key_condition.alwaysUnknownOrTrue())
    {
        throw Exception(
            ErrorCodes::INDEX_NOT_USED,
            "Primary key ({}) is not used and setting 'force_primary_key' is set.",
            fmt::join(primary_key_columns, ", "));
    }
    LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

    const auto & select = query_info.query->as<ASTSelectQuery &>();

    MergeTreeDataSelectExecutor::filterPartsByPartition(
        parts, part_values, metadata_snapshot, storage, query_info, context,
        max_block_numbers_to_read.get(), log, result.index_stats);

    result.sampling = MergeTreeDataSelectExecutor::getSampling(
        select, metadata_snapshot->getColumns().getAllPhysical(), parts, key_condition,
        storage, metadata_snapshot, context, sample_factor_column_queried, log);

    if (result.sampling.read_nothing)
        return result;

    size_t total_marks_pk = 0;
    for (const auto & part : parts)
        total_marks_pk += part->index_granularity.getMarksCountWithoutFinal();

    size_t parts_before_pk = parts.size();

    result.parts_with_ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
        std::move(parts),
        metadata_snapshot,
        query_info,
        context,
        key_condition,
        reader_settings,
        log,
        requested_num_streams,
        result.index_stats,
        true);

    size_t sum_marks_pk = total_marks_pk;
    for (const auto & stat : result.index_stats)
        if (stat.type == ReadFromMergeTree::IndexType::PrimaryKey)
            sum_marks_pk = stat.num_granules_after;

    size_t sum_marks = 0;
    size_t sum_ranges = 0;

    for (const auto & part : result.parts_with_ranges)
    {
        sum_ranges += part.ranges.size();
        sum_marks += part.getMarksCount();
    }

    LOG_DEBUG(
        log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        parts_before_pk,
        total_parts,
        result.parts_with_ranges.size(),
        sum_marks_pk,
        total_marks_pk,
        sum_marks,
        sum_ranges);

    //ProfileEvents::increment(ProfileEvents::SelectedParts, result.parts_with_ranges.size());
    //ProfileEvents::increment(ProfileEvents::SelectedRanges, sum_ranges);
    //ProfileEvents::increment(ProfileEvents::SelectedMarks, sum_marks);

    const auto & input_order_info = query_info.input_order_info
        ? query_info.input_order_info
        : (query_info.projection ? query_info.projection->input_order_info : nullptr);

    if ((settings.optimize_read_in_order || settings.optimize_aggregation_in_order) && input_order_info)
        result.read_type = (input_order_info->direction > 0) ? ReadFromMergeTree::ReadType::InOrder
                                                             : ReadFromMergeTree::ReadType::InReverseOrder;

    return result;
}

static void selectColumnNames(
    const Names & column_names_to_return,
    const MergeTreeData & data,
    Names & real_column_names,
    Names & virt_column_names,
    bool & sample_factor_column_queried)
{
    sample_factor_column_queried = false;

    for (const String & name : column_names_to_return)
    {
        if (name == "_part")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_part_index")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_partition_id")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_part_uuid")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_partition_value")
        {
            if (!typeid_cast<const DataTypeTuple *>(data.getPartitionValueType().get()))
            {
                throw Exception(
                    ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                    "Missing column `_partition_value` because there is no partition column in table {}",
                    data.getStorageID().getTableName());
            }

            virt_column_names.push_back(name);
        }
        else if (name == "_sample_factor")
        {
            sample_factor_column_queried = true;
            virt_column_names.push_back(name);
        }
        else
        {
            real_column_names.push_back(name);
        }
    }
}

void doTableScanForGRPC(ContextMutablePtr& query_context, QueryPlan & query_plan, GRPCTableScanStep table_scan_step)
{
    Poco::Logger * log = &Poco::Logger::get("GRPCForQueryPlan");
    const Settings & settings = query_context->getSettingsRef();

    auto select_ast_ptr = std::make_shared<ASTSelectQuery>();

    const char * begin_proj = table_scan_step.projection().data();
    const char * end_proj = begin_proj + table_scan_step.projection().size();
    ParserNotEmptyExpressionList parser_proj(true);
    ASTPtr proj_ast = parseQuery(parser_proj, begin_proj, end_proj, "", settings.max_query_size, settings.max_parser_depth);
    select_ast_ptr->setExpression(ASTSelectQuery::Expression::SELECT,
            parseQuery(parser_proj,
                    begin_proj,
                    end_proj, "",
                    settings.max_query_size,
                    settings.max_parser_depth));

//    const char * begin_filter = table_scan_step.filter().data();
//    const char * end_filter = begin_filter + table_scan_step.filter().size();
//    ParserExpressionWithOptionalAlias parser_filter(false);
//    ASTPtr filter_ast = parseQuery(parser_filter, begin_filter, end_filter, "", settings.max_query_size, settings.max_parser_depth);
//    select_ast_ptr->setExpression(ASTSelectQuery::Expression::WHERE,
//            parseQuery(parser_filter,
//                    begin_filter,
//                    end_filter, "",
//                    settings.max_query_size,
//                    settings.max_parser_depth));

    const char * begin_prewhere = table_scan_step.filter().data();
    const char * end_prewhere = begin_prewhere + table_scan_step.filter().size();
    ParserExpressionWithOptionalAlias parser_prewhere(false);
    ASTPtr prewhere_ast = parseQuery(parser_prewhere, begin_prewhere, end_prewhere, "", settings.max_query_size, settings.max_parser_depth);
    select_ast_ptr->setExpression(ASTSelectQuery::Expression::PREWHERE,
            parseQuery(parser_prewhere,
                    begin_prewhere,
                    end_prewhere, "",
                    settings.max_query_size,
                    settings.max_parser_depth));

    const char * begin_tables = table_scan_step.table_name().data();
    const char * end_tables = begin_tables + table_scan_step.table_name().size();
    ParserTablesInSelectQuery parser_tables;
    ASTPtr table_ast = parseQuery(parser_tables, begin_tables, end_tables, "", settings.max_query_size, settings.max_parser_depth);
    select_ast_ptr->setExpression(ASTSelectQuery::Expression::TABLES,
            parseQuery(parser_tables,
                    begin_tables,
                    end_tables, "",
                    settings.max_query_size,
                    settings.max_parser_depth));

    auto tables_in_select_query = table_ast->as<ASTTablesInSelectQuery>();
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

    if (!DatabaseCatalog::instance().isTableExist(table_id, query_context))
        throw Exception("Table " + table_id.getTableName() + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

    StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, query_context);
    StorageMergeTree * mergetree_storage = dynamic_cast<StorageMergeTree *>(storage.get());
    if(!mergetree_storage)
    {
        throw Exception("Support MergeTree table only.", ErrorCodes::UNKNOWN_TABLE);
    }

    StorageMetadataPtr metadata_snapshot = mergetree_storage->getInMemoryMetadataPtr();

    auto proj_analyzer_result = TreeRewriterResult(metadata_snapshot->getSampleBlock().getNamesAndTypesList());
    proj_analyzer_result.collectUsedColumns(proj_ast, false);
    Names req_column_names = proj_analyzer_result.required_source_columns.getNames();
    for(auto col_name : req_column_names)
    {
        LOG_INFO(log, "-----------3333-------{}", col_name);
    }
    ASTPtr query_ast = std::move(select_ast_ptr);

    WriteBufferFromOwnString write_buf;
    formatAST(*query_ast, write_buf, false, false);
    LOG_INFO(log, "-----query is : {}", write_buf.str());

    SelectQueryInfo query_info;
    //query_info.query = select_ast_ptr;
    query_info.query = query_ast;
    query_info.syntax_analyzer_result = TreeRewriter(query_context)
            .analyzeSelect(query_ast, TreeRewriterResult({}, storage, metadata_snapshot));

    SettingsChanges settings_changes;
    settings_changes.push_back({"max_ast_depth", 1000});
    query_context->checkSettingsConstraints(settings_changes);
    query_context->applySettingsChanges(settings_changes);

    auto syntax_reulst = TreeRewriter(query_context).analyze(prewhere_ast, metadata_snapshot->getSampleBlock().getNamesAndTypesList());
    ExpressionAnalyzer analyzer(prewhere_ast, syntax_reulst, query_context);
    //auto expression_actions = analyzer.getActions(false, true);

    query_info.prewhere_info = std::make_shared<PrewhereInfo>(analyzer.getActionsDAG(false), prewhere_ast->getColumnName());

    Names real_column_names;
    Names virt_column_names;
    bool sample_factor_column_queried = false;
    selectColumnNames(req_column_names, *mergetree_storage,
            real_column_names, virt_column_names, sample_factor_column_queried);


    TableScan::AnalysisResult analysis_result = selectRangesToRead(
            *mergetree_storage, query_context, real_column_names,
            settings.max_threads, query_info,log);

    auto table_scan = std::make_unique<TableScan>(
            analysis_result,
            MergeTreeBaseSelectProcessor::transformHeader(
                    metadata_snapshot->getSampleBlockForColumns(real_column_names, storage->getVirtuals(), table_id),
                    query_info.prewhere_info, mergetree_storage->getPartitionValueType(),
                    virt_column_names),
            virt_column_names, false, nullptr,
            query_info.prewhere_info, *mergetree_storage, metadata_snapshot,
            metadata_snapshot, query_context, settings.max_block_size,
            8, false, nullptr, log);

    query_plan.addStep(std::move(table_scan));

    LOG_INFO(log, "-----------begin test with gporca integration-------");
    auto query_to_dxl_translator = std::make_unique<TranslatorQueryToDXL>();
    LOG_INFO(log, "-----------end test with gporca integration-------");
}

//void doTableScanForGRPC([[maybe_unused]] ContextMutablePtr& query_context, [[maybe_unused]] QueryPlan & query_plan, [[maybe_unused]] GRPCTableScanStep table_scan_step)
//{
//    Poco::Logger * log = &Poco::Logger::get("GRPCForQueryPlan");
//    const Settings & settings = query_context->getSettingsRef();
//
//    const char * begin_filter = table_scan_step.filter().data();
//    const char * end_filter = begin_filter + table_scan_step.filter().size();
//    ParserExpressionWithOptionalAlias parser_filter(false);
//    auto select_ast_ptr = std::make_shared<ASTSelectQuery>();
//    select_ast_ptr->setExpression(ASTSelectQuery::Expression::WHERE, parseQuery(parser_filter, begin_filter, end_filter, "", settings.max_query_size, settings.max_parser_depth));
//
//    const char * begin_proj = table_scan_step.projection().data();
//    const char * end_proj = begin_proj + table_scan_step.projection().size();
//    ParserNotEmptyExpressionList parser_proj(true);
//    select_ast_ptr->setExpression(ASTSelectQuery::Expression::SELECT, parseQuery(parser_proj, begin_proj, end_proj, "", settings.max_query_size, settings.max_parser_depth));
//
//    const char * begin_tables = table_scan_step.table_name().data();
//    const char * end_tables = begin_tables + table_scan_step.table_name().size();
//    ParserTablesInSelectQuery parser_tables;
//    select_ast_ptr->setExpression(ASTSelectQuery::Expression::TABLES, parseQuery(parser_tables, begin_tables, end_tables, "", settings.max_query_size, settings.max_parser_depth));
//
//    auto tables_in_select_query = select_ast_ptr->tables()->as<ASTTablesInSelectQuery>();
//    StorageID table_id = StorageID::createEmpty();
//    if(!tables_in_select_query->children.empty())
//    {
//        auto tables_element = tables_in_select_query->children[0]->as<ASTTablesInSelectQueryElement>();
//        if(tables_element->table_expression)
//        {
//            auto table_expression = tables_element->table_expression->as<ASTTableExpression>();
//            if(table_expression && table_expression->database_and_table_name)
//            {
//                auto db_and_table_name = table_expression->database_and_table_name->as<ASTTableIdentifier>();
//                if(db_and_table_name)
//                {
//                    table_id = StorageID(*db_and_table_name);
//                    LOG_INFO(log, "-----------222222bbb-------{}----{}", db_and_table_name->getTableId().database_name, db_and_table_name->getTableId().table_name);
//                }
//            }
//        }
//    }
//
//    if(table_id.empty())
//    {
//        throw Exception("Can not get table name.", ErrorCodes::UNKNOWN_TABLE);
//    }
//
//    if (!DatabaseCatalog::instance().isDatabaseExist(table_id.getDatabaseName()))
//        throw Exception("Database " + table_id.getDatabaseName() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
//
//    WriteBufferFromOwnString write_buf;
//    formatAST(*select_ast_ptr, write_buf, false, false);
//    LOG_INFO(log, "-----------222222ccc-------{}", write_buf.str());
//
//    SelectQueryInfo query_info;
//    query_info.query = select_ast_ptr;
//
//    if (!DatabaseCatalog::instance().isTableExist(table_id, query_context))
//        throw Exception("Table " + table_id.getTableName() + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);
//
//    ASTPtr query_ast = std::move(select_ast_ptr);
//    auto interpreter = InterpreterFactory::get(query_ast, query_context, SelectQueryOptions(QueryProcessingStage::Complete));
//    if(auto * select_interpreter = typeid_cast<InterpreterSelectQuery *>(&*interpreter))
//    {
//        select_interpreter->buildQueryPlan(query_plan);
//    }
//}

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

void testRestoreQualifiedNamesVisitor(ContextMutablePtr& query_context, Poco::Logger * log, std::string query_text)
{
    const Settings & settings = query_context->getSettingsRef();

    //std::string sql_test = "select t.a,t.b,concat(1+2,t.c) from local.test t";
    const char * begin_test = query_text.data();
    const char * end_test = begin_test + query_text.size();
    ParserSelectQuery parser_test;
    ASTPtr query_ast_test = parseQuery(parser_test, begin_test, end_test, "", settings.max_query_size, settings.max_parser_depth);

    RestoreQualifiedNamesVisitor::Data data;
    data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query_ast_test->as<ASTSelectQuery&>(), 0));
    data.remote_table.database = "remote_db";
    data.remote_table.table = "remove_table";
    data.rename = true;
    RestoreQualifiedNamesVisitor(data).visit(query_ast_test);

    WriteBufferFromOwnString write_buf;
    formatAST(*query_ast_test, write_buf, false, false);
    LOG_INFO(log, "-----query after rewrite is : {}", write_buf.str());
}

}

