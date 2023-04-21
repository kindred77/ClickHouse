#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnArray.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/**
  * For each group of primary keys(data sorted by sorting keys), merge to the base row partially,
  *  delete when met index 0.
  *  Do not support vertical merge algorithm recently.
  */
class PartialReplacingSortedAlgorithm final : public IMergingAlgorithmWithSharedChunks
{
public:
    PartialReplacingSortedAlgorithm(
        const Block & header_, size_t num_inputs,
        SortDescription description_, const String & part_replacing_colnames_colname_,
        Names primary_key_columns_,
        Names all_column_names,
        size_t max_block_size,
        const String& delete_flag_ = "_delete_flag",
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    Status merge() override;

private:
    using RowRef = detail::RowRefWithOwnedChunk;
    using ReplacingNames = std::vector<String>;
    using Indexes = std::vector<size_t>;
    //using ColNameToPosMap = std::unordered_map<String, size_t>;
    using UniqueString = std::set<String>;
    static constexpr size_t max_row_refs = 2; /// last, current.

    std::tuple<String, size_t> part_replacing_col_info;
    Indexes primary_keys_pos;

    RowRef base_row;
    ColumnRawPtrs all_columns;
    Indexes row_nums;
    size_t owned_chunk_row_num = 0;
    //convert column name to position in block
    //ColNameToPosMap colname_to_pos;
    UniqueString merged_unique_colnames;
    bool if_partial_replaced = false;
    Block header;
    String delete_flag;

    /// Sources of rows with the current primary key.
    //PODArray<RowSourcePart> current_row_sources;

    void insertRow();

    size_t sizeAt(const IColumn* column_ptr, size_t i);

    bool hasEqualPrimaryKeyColumnsWithBaseRow(SortCursor current);

    ReplacingNames extractReplacingColunmNames(const IColumn* column_ptr, size_t row);

    void replacePartially(const ReplacingNames& current_indexes, SortCursor current);

    void setBaseRow(SortCursor current)
    {
        clear();
        setRowRef(base_row, current);
        owned_chunk_row_num = sources[current->order].chunk->getNumRows();
    }

    bool invalid(const String& col_name)
    {
        return col_name == ""
                || !header.has(col_name)
                //do not replace partial indexes column
                || col_name == std::get<0>(part_replacing_col_info)
                //do not update primary keys
                || std::find(primary_keys_pos.begin(), primary_keys_pos.end(), header.getPositionByName(col_name)) != primary_keys_pos.end();
    }

    void clear()
    {
        all_columns.clear();
        row_nums.clear();
        base_row.clear();
        base_row.owned_chunk = nullptr;
        owned_chunk_row_num = 0;
        merged_unique_colnames.clear();
        if_partial_replaced = false;
    }

    struct PartialReplacingMergedData : public MergedData
    {
    public:
        PartialReplacingMergedData(MutableColumns columns_, bool use_average_block_size_, UInt64 max_block_size_)
                : MergedData(std::move(columns_), use_average_block_size_, max_block_size_)
        {
        }
        void insertRowWithPartial(const ColumnRawPtrs & raw_columns,
                std::vector<size_t> rows,
                String part_replacing_colnames_colname,
                UniqueString merged_unique_colnames,
                size_t block_size);
    };

    PartialReplacingMergedData merged_data;
};

}
