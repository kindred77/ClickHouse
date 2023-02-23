#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-private-field"
#else
#pragma GCC diagnostic ignored "-Wunused-private-field"
#endif

namespace DB
{

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

class Pipe;
extern MergeTreeReaderSettings getMergeTreeReaderSettings(const ContextPtr & context);

class TableScan final : public ISourceStep
{
public:

    struct AnalysisResult
    {
        RangesInDataParts parts_with_ranges;
        MergeTreeDataSelectSamplingData sampling;
        ReadFromMergeTree::IndexStats index_stats;
        Names column_names_to_read;
        ReadFromMergeTree::ReadType read_type = ReadFromMergeTree::ReadType::Default;
    };

    TableScan(
        AnalysisResult analysis_result_,
        Block header,
        //MergeTreeData::DataPartsVector parts_,
        //Names real_column_names_,
        Names virt_column_names_,
        bool is_final_,
        InputOrderInfoPtr input_order_info_,
        PrewhereInfoPtr prewhere_info_,
        const MergeTreeData & data_,
        //const SelectQueryInfo & query_info_,
        StorageMetadataPtr metadata_snapshot_,
        StorageMetadataPtr metadata_snapshot_base_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        bool sample_factor_column_queried_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        Poco::Logger * log_
    );

    String getName() const override { return "TableScan"; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;

private:
    AnalysisResult analysis_result;
    //MergeTreeData::DataPartsVector prepared_parts;
    //Names real_column_names;
    Names virt_column_names;
    bool is_final = false;
    InputOrderInfoPtr input_order_info;
    PrewhereInfoPtr prewhere_info;
    const MergeTreeData & data;
    //SelectQueryInfo query_info;
    //PrewhereInfoPtr prewhere_info;
    StorageMetadataPtr metadata_snapshot;
    StorageMetadataPtr metadata_snapshot_base;

    const MergeTreeReaderSettings reader_settings;

    ExpressionActionsSettings actions_settings;

    ContextPtr context;

    const size_t max_block_size;
    const size_t requested_num_streams;
    const size_t preferred_block_size_bytes;
    const size_t preferred_max_column_in_block_size_bytes;
    const bool sample_factor_column_queried;

    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;

    Poco::Logger * log;

    Pipe read(RangesInDataParts parts_with_range, Names required_columns, ReadFromMergeTree::ReadType read_type, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readFromPool(RangesInDataParts parts_with_ranges, Names required_columns, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readInOrder(RangesInDataParts parts_with_range, Names required_columns, ReadFromMergeTree::ReadType read_type, bool use_uncompressed_cache);

    template<typename TSource>
    ProcessorPtr createSource(const RangesInDataPart & part, const Names & required_columns, bool use_uncompressed_cache);

    Pipe spreadMarkRangesAmongStreams(
        RangesInDataParts && parts_with_ranges,
        const Names & column_names);

    Pipe spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts_with_ranges,
        const Names & column_names,
        const ActionsDAGPtr & sorting_key_prefix_expr,
        ActionsDAGPtr & out_projection/*,
        const InputOrderInfoPtr & input_order_info*/);

    Pipe spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        const Names & column_names,
        ActionsDAGPtr & out_projection);
    //AnalysisResult selectRangesToRead(MergeTreeData::DataPartsVector parts) const;
};

}
