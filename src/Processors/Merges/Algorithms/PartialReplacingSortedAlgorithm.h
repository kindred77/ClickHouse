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

    size_t sizeAt(const IColumn* column_ptr, size_t i)
    {
        if (const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(column_ptr))
        {
            return column_array->getOffsets()[i] - column_array->getOffsets()[i - 1];
        }

        return 0;
    }

    bool hasEqualPrimaryKeyColumnsWithBaseRow(SortCursor current)
    {
        for (auto pos : primary_keys_pos)
        {
            auto & cur_column = current->all_columns[pos];
            auto & other_column = (*base_row.all_columns)[pos];

            if (0 != cur_column->compareAt(current->getRow(), base_row.row_num, *other_column, 1))
                return false;
        }

        return true;
    }

    Indexes extractIndexesFromColumnArray(const IColumn* column_ptr, size_t row)
    {
        Indexes result;
        size_t part_col_indexes_size = sizeAt(column_ptr, row);
        if (!part_col_indexes_size) return result;
        if (const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(column_ptr))
        {
            const ColumnUInt16 * col_uint16 = checkAndGetColumn<ColumnUInt16>(column_array->getData());
            auto offset = column_array->getOffsets()[row - 1];
            result.reserve(part_col_indexes_size);
            for (size_t i = 0; i < part_col_indexes_size; ++i)
            {
                size_t idx = col_uint16->getUInt(offset + i);
                result.push_back(idx);
            }
        }

        return result;
    }

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

    void replacePartially(Indexes current_indexes, SortCursor current)
    {
        Indexes base_row_indexes = extractIndexesFromColumnArray((*base_row.all_columns)[part_cols_indexes_column_pos], base_row.row_num);
        bool base_row_is_partial = !base_row_indexes.empty();
        for (auto idx : current_indexes)
        {
            if (invalid(idx)) continue;
            //if base row is a partial columns row, we merge all the indexes into base row
            if (base_row_is_partial)
            {
                //initialize
                if (merged_unique_indexes.empty())
                {
                    for (auto idx_in_base : base_row_indexes)
                    {
                        if (invalid(idx_in_base)) continue;
                        merged_unique_indexes.insert(idx_in_base);
                    }
                }
                merged_unique_indexes.insert(idx);
            }
            //entering partial column merging mode
            if (all_columns.empty())
            {
                all_columns = {base_row.all_columns->begin(), base_row.all_columns->end()};
                row_nums.assign(all_columns.size(), base_row.row_num);
            }
            if_partial_replaced = true;
            //position in block/chunk
            size_t pos=index_to_pos[idx - 1];
            //replace partially
            all_columns[pos] = current->all_columns[pos];
            row_nums[pos] = current->getRow();
        }
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
