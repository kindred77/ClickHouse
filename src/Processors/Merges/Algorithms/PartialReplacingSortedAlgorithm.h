#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Columns/ColumnsNumber.h>
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
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & part_cols_indexes_column,
        Names primary_key_columns_,
        Names all_column_names,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    Status merge() override;

private:
    using RowRef = detail::RowRefWithOwnedChunk;
    using Indexes = std::vector<size_t>;
    using UniqueIndexes = std::set<size_t>;
    static constexpr size_t max_row_refs = 2; /// last, current.

    size_t part_cols_indexes_column_pos = 0;
    Indexes primary_keys_pos;

    RowRef base_row;
    ColumnRawPtrs all_columns;
    Indexes row_nums;
    size_t owned_chunk_row_num = 0;
    //convert user given column index to position in block
    Indexes index_to_pos;
    UniqueIndexes merged_unique_indexes;
    bool if_partial_replaced = false;

    /// Sources of rows with the current primary key.
    //PODArray<RowSourcePart> current_row_sources;

    void insertRow();

    size_t sizeAt(const IColumn* column_ptr, size_t i);

    bool hasEqualPrimaryKeyColumnsWithBaseRow(SortCursor current);

    Indexes extractIndexesFromColumnArray(const IColumn* column_ptr, size_t row);

    void replacePartially(Indexes current_indexes, SortCursor current);

    void setBaseRow(SortCursor current)
    {
        clear();
        setRowRef(base_row, current);
        owned_chunk_row_num = sources[current->order].chunk->getNumRows();
    }

    void clear()
    {
        all_columns.clear();
        row_nums.clear();
        base_row.clear();
        base_row.owned_chunk = nullptr;
        owned_chunk_row_num = 0;
        merged_unique_indexes.clear();
        if_partial_replaced = false;
    }

    bool invalid(size_t idx)
    {
        return idx == 0
                || idx > index_to_pos.size()
                //do not replace partial indexes column
                || index_to_pos[idx - 1] == part_cols_indexes_column_pos
                //do not update primary keys
                || std::find(primary_keys_pos.begin(), primary_keys_pos.end(), index_to_pos[idx - 1]) != primary_keys_pos.end();
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
                size_t part_cols_indexes_column_pos,
                std::set<size_t> merged_unique_indexes,
                size_t block_size);
    };

    PartialReplacingMergedData merged_data;
};

}
