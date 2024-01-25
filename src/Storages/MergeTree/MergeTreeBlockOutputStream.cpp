#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageSet.h>
#include <Interpreters/PartLog.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

Block MergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}


void MergeTreeBlockOutputStream::writePrefix()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded();
}

// void MergeTreeBlockOutputStream::upsert(const String & part_name, const Block & block)
// {
//     if (storage.getDataPartsVector().size() == 0)
//     {
//         return;
//     }
//     Stopwatch watch;
//     Poco::Logger * log = &Poco::Logger::get("MergeTreeBlockOutputStream::upsert");
//     LOG_INFO(log, "-----00000------------");
//     auto query_context = Context::createCopy(context);
//     const Settings & settings = query_context->getSettingsRef();
//     //select list
//     const auto select_ast_ptr = std::make_shared<ASTSelectQuery>();
//     select_ast_ptr->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
//     const auto select_expression_list = select_ast_ptr->select();
//     Names real_column_names = {"id"};
//     Names virt_column_names = {"_part", "_mark", "_offset_in_mark", "_valid_flag"};
//     select_expression_list->children.reserve(real_column_names.size() + virt_column_names.size());
//     for (const auto & name : real_column_names)
//         select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(name));
//     for (const auto & name : virt_column_names)
//         select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(name));
//     LOG_INFO(log, "-----11111------------");
//     //from table
//     StoragePtr storage_ptr = DatabaseCatalog::instance().getTable(storage.getStorageID(), query_context);
//     if (!storage_ptr)
//     {
//         throw Exception("Table not exists, may be it's a bug.", ErrorCodes::UNKNOWN_TABLE);
//     }
//     StorageMergeTree * mergetree_storage = dynamic_cast<StorageMergeTree *>(storage_ptr.get());
//     if(!mergetree_storage)
//     {
//         throw Exception("Support MergeTree table only.", ErrorCodes::UNKNOWN_TABLE);
//     }
//     select_ast_ptr->replaceDatabaseAndTable(storage.getStorageID());
    
//     //prewhere
//     String tmp_tab_name = "_tmp_table_1";
//     String prewhere_sql = "id in(" + tmp_tab_name + ")";

//     //prepare and add temporary table for in expr
//     NamesAndTypesList columns = block.getNamesAndTypesList();
//     DiskPtr disk = context->getDisk("default");
//     //auto table_uuid = storage.getStorageID().
//     TemporaryTableHolder external_storage_holder(
//         query_context,
//         [&disk, &columns](const StorageID & table_id)
//         {
//             //escapeForFileName
//             auto storage_set = StorageSet::create(disk, "notexists/", table_id, ColumnsDescription{columns}, ConstraintsDescription{}, String{}, false, true);
//             return storage_set;
//         },
//         nullptr);

//     StoragePtr external_table = external_storage_holder.getTable();
//     auto table_out = external_table->write({}, external_table->getInMemoryMetadataPtr(), query_context);
//     table_out->writePrefix();
//     table_out->write(block);
//     table_out->writeSuffix();
//     query_context->addExternalTable(tmp_tab_name, std::move(external_storage_holder));

//     const char * begin_prewhere = prewhere_sql.c_str();
//     const char * end_prewhere = prewhere_sql.c_str() + prewhere_sql.size();
//     ParserExpressionWithOptionalAlias parser_prewhere(false);
//     auto prewhere_ast = parseQuery(parser_prewhere,
//                     begin_prewhere,
//                     end_prewhere, "",
//                     settings.max_query_size,
//                     settings.max_parser_depth);
//     auto syntax_reulst_prewhere = TreeRewriter(query_context).analyze(prewhere_ast, metadata_snapshot->getSampleBlock().getNamesAndTypesList());
//     select_ast_ptr->setExpression(ASTSelectQuery::Expression::PREWHERE,
//             std::move(prewhere_ast));
//     LOG_INFO(log, "-----22222------------");
//     WriteBufferFromOwnString write_buf;
//     formatAST(*select_ast_ptr, write_buf, false, false);
//     LOG_INFO(log, "-----generated query is : {}", write_buf.str());

//     ASTPtr query_ast = std::move(select_ast_ptr);
//     SelectQueryInfo query_info;
//     query_info.query = query_ast;
//     query_info.syntax_analyzer_result = TreeRewriter(query_context)
//             .analyzeSelect(query_ast, TreeRewriterResult({}, storage_ptr, metadata_snapshot));
//     LOG_INFO(log, "-----33333------------");
//     ExpressionAnalyzer analyzer(prewhere_ast, syntax_reulst_prewhere, query_context);
//     //SelectQueryExpressionAnalyzer analyzer(prewhere_ast, syntax_reulst_prewhere, query_context, metadata_snapshot, {}, false, {}, {}, std::move(prepared_sets));
//     LOG_INFO(log, "-----4444------------");
//     query_info.prewhere_info = std::make_shared<PrewhereInfo>(analyzer.getActionsDAG(false), prewhere_ast->getColumnName());
//     query_info.sets = analyzer.getPreparedSets();
//     // query_info.sets = prepared_sets;
//     LOG_INFO(log, "-----5555------------query_info.sets.size(): {}", query_info.sets.size());
    
//     auto parts = storage.getDataPartsVector();
//     LOG_INFO(log, "-----6666------------parts.size:{}-----block.rows():{}", parts.size(), block.rows());
//     auto read_from_merge_tree = std::make_unique<ReadFromMergeTree>(
//         parts,
//         real_column_names,
//         virt_column_names,
//         storage,
//         query_info,
//         metadata_snapshot,
//         metadata_snapshot,
//         query_context,
//         block.rows(),
//         2,
//         false,
//         nullptr,
//         &Poco::Logger::get("ReadFromMergeTree")
//     );
//     LOG_INFO(log, "-----7777------------");
//     QueryPlanPtr plan = std::make_unique<QueryPlan>();
//     plan->addStep(std::move(read_from_merge_tree));

//     auto pipeline = plan->buildQueryPipeline(
//                             QueryPlanOptimizationSettings::fromContext(query_context), 
//                             BuildQueryPipelineSettings::fromContext(query_context));
//     LOG_INFO(log, "-----8888------------");
//     //TODO order by _part,_mark
//     PullingAsyncPipelineExecutor executor(*pipeline);
//     LOG_INFO(log, "-----999------------");
//     Block block_res;
//     while (executor.pull(block_res))
//     {
//         LOG_INFO(log, "-----aaa------------");
//         if (block_res.rows() == 0)
//         {
//             continue;
//         }
//         auto id_col = block_res.getByName("id").column;
//         auto _part_col = block_res.getByName("_part").column;

//         //check part name consistency
//         for (size_t i = 0; i < _part_col->size(); ++i)
//         {
//             const auto & got_part_name = _part_col->operator[](i);
//             if (part_name != got_part_name)
//             {
//                 throw Exception("Partition name is not consistency, it is a bug, got: " + got_part_name.get<String>() + ", expected: " + part_name, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
//             }
//         }

//         auto _mark_col = block_res.getByName("_mark").column;
//         auto _offset_in_mark_col = block_res.getByName("_offset_in_mark").column;
//         auto _valid_flag_col = block_res.getByName("_valid_flag").column;
//         LOG_INFO(log, "-----bbb------------");
//         for (size_t i : collections::range(0, block_res.rows()))
//         {
//             LOG_INFO(log, "-----ccc------------");
//             LOG_INFO(log, "-----id:{}, _part: {}, _mark:{}, _offset_in_mark:{}, _valid_flag:{}------------",
//                 id_col->operator [](i), part_name, _mark_col->operator [](i),
//                 _offset_in_mark_col->operator [](i), _valid_flag_col->operator [](i));
//             LOG_INFO(log, "-----ddd------------");
//         }
//         //do not care about primary keys, valid flags
//         mergetree_storage->markDeleted(part_name, _mark_col, _offset_in_mark_col);
//     }
//     LOG_INFO(log, "-----eee------------");
//     watch.stop();
//     LOG_INFO(log, "Upsert processed in {} sec", watch.elapsedSeconds());
// }

// void MergeTreeBlockOutputStream::insertMarks(const MergeTreeData::MutableDataPartPtr & part)
// {
//     const auto & index_granu = part->index_granularity;
//     const auto & part_name = part->name;
//     //StoragePtr storage_ptr = DatabaseCatalog::instance().getTable(storage.getStorageID(), context);
//     //StorageMergeTree * mergetree_storage = dynamic_cast<StorageMergeTree *>(storage_ptr.get());
//     //mergetree_storage->newMarks(part_name, index_granu);
//     storage.newMarks(part_name, index_granu);
// }

void MergeTreeBlockOutputStream::write(const Block & block)
{
    const auto storage_settings = storage.getSettings();
    //TODO move to initializing method
    std::optional<Names> pk_cols;
    if (storage_settings->enable_unique_mode)
    {
        pk_cols = metadata_snapshot->getPrimaryKeyColumns();
    }
    
    Stopwatch watch;
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);
    watch.stop();
    LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::write"), "-----splitBlockIntoParts processed in {} sec.", watch.elapsedSeconds());
    for (auto & current_block : part_blocks)
    {
        watch.restart();
        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);
        watch.stop();
        LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::write"), "-----writeTempPart processed in {} sec.", watch.elapsedSeconds());
        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!part)
            continue;

        bool rename_and_add_flag = false;
        if (storage_settings->enable_unique_mode)
        {
            LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::upsert"), "-----0000------------");
            Block mark_offset_cols;
            Block block_pk;
            for (const auto & pk_col_name : pk_cols.value())
            {
                block_pk.insert(current_block.block.getByName(pk_col_name));
            }
            LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::upsert"), "-----1111------------");
            watch.restart();
            if (rename_and_add_flag = storage.renameTempPartAndAdd(part, &storage.increment, nullptr, storage.getDeduplicationLog(), &mark_offset_cols, block_pk);
                rename_and_add_flag)
            {
                watch.stop();
                LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::write"), "-----renameTempPartAndAdd processed in {} sec.", watch.elapsedSeconds());
                watch.restart();
                storage.newMarks(part);
                watch.stop();
                LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::write"), "-----insertMarks processed in {} sec.", watch.elapsedSeconds());
            }

            if (mark_offset_cols.rows() > 0)
            {
                watch.restart();
                storage.markDeleted(
                    mark_offset_cols.safeGetByPosition(0).column,
                    mark_offset_cols.safeGetByPosition(1).column,
                    mark_offset_cols.safeGetByPosition(2).column);
                watch.stop();
                LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::write"), "-----markDeleted processed in {} sec.", watch.elapsedSeconds());
            }
            
            LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::upsert"), "-----4444------------mark_offset_cols.rows():{}", mark_offset_cols.rows());
        }
        else
        {
            LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::upsert"), "-----5555------------");
            rename_and_add_flag = storage.renameTempPartAndAdd(part, &storage.increment, nullptr, storage.getDeduplicationLog());
        }
        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (rename_and_add_flag)
        {
            PartLog::addNewPart(storage.getContext(), part, watch.elapsed());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_executor.triggerTask();
        }
        LOG_INFO(&Poco::Logger::get("MergeTreeBlockOutputStream::upsert"), "-----6666------------");
    }
}

}
