#include <Processors/Merges/Algorithms/PartialReplacingSortedAlgorithm.h>

namespace DB
{

void PartialReplacingSortedAlgorithm::PartialReplacingMergedData::insertRowWithPartial(const ColumnRawPtrs & raw_columns,
        std::vector<size_t> rows,
        size_t part_cols_indexes_column_pos,
        std::set<size_t> merged_unique_indexes,
        size_t block_size)
{
    size_t num_columns = raw_columns.size();
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (!merged_unique_indexes.empty() && part_cols_indexes_column_pos == i)
        {
            const Field & res = Array(merged_unique_indexes.begin(), merged_unique_indexes.end());
            columns[i]->insert(res);
        }
        else
        {
            columns[i]->insertFrom(*raw_columns[i], rows[i]);
        }

    }

    ++total_merged_rows;
    ++merged_rows;
    sum_blocks_granularity += block_size;
}

PartialReplacingSortedAlgorithm::PartialReplacingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & part_cols_indexes_column,
        Names primary_key_columns_,
        Names all_column_names,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_,
        bool use_average_block_sizes)
        : IMergingAlgorithmWithSharedChunks(num_inputs, std::move(description_), out_row_sources_buf_, max_row_refs)
        , merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
{
    for (auto & column_name : primary_key_columns_)
    {
        primary_keys_pos.push_back(header.getPositionByName(column_name));
    }

    part_cols_indexes_column_pos = header.getPositionByName(part_cols_indexes_column);

    //initialize index to position converter
    index_to_pos.reserve(all_column_names.size());
    for (size_t i = 0; i < all_column_names.size(); ++i)
    {
        index_to_pos.push_back(header.getPositionByName(all_column_names[i]));
    }
}

void PartialReplacingSortedAlgorithm::insertRow()
{
    if (if_partial_replaced)
    {
        merged_data.insertRowWithPartial(all_columns, row_nums, part_cols_indexes_column_pos, merged_unique_indexes, owned_chunk_row_num);
    }
    else
    {
        merged_data.insertRow(*base_row.all_columns, base_row.row_num, owned_chunk_row_num);
    }
    clear();
}

IMergingAlgorithm::Status PartialReplacingSortedAlgorithm::merge()
{
    while (queue.isValid())
    {
        SortCursor current = queue.current();

        if (current->isLast() && skipLastRowFor(current->order))
        {
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        //data ordered by sorting columns, merged by primary key columns.
        bool key_differs = base_row.empty() || !hasEqualPrimaryKeyColumnsWithBaseRow(current);
        if (key_differs)
        {
            /// if there are enough rows and the last one is calculated completely
            if (merged_data.hasEnoughRows())
                return Status(merged_data.pull());

            /// Write the data for the previous primary key.
            if (!base_row.empty())
            {
                insertRow();
            }
        }

        //got a base row, may be a full columns row or partial columns row.
        if (base_row.empty())
        {
            setBaseRow(current);
        }
        else
        {
            //size_t part_col_indexes_size=sizeAt(current->all_columns[part_cols_indexes_column_pos], current->getRow());
            Indexes indexes = extractIndexesFromColumnArray(current->all_columns[part_cols_indexes_column_pos], current->getRow());
            //partial columns row, replace the base row with it.
            if (!indexes.empty())
            {
                //delete
                if (indexes.size() == 1 && indexes[0] == 0)
                {
                    clear();
                }
                else
                {
                    replacePartially(indexes, current);
                }
            }
            //full columns row, set it as a base row
            else
            {
                setBaseRow(current);
            }
        }

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    /// If have enough rows, return block, because it prohibited to overflow requested number of rows.
    if (merged_data.hasEnoughRows())
        return Status(merged_data.pull());

    /// We will write the data for the last primary key.
    if (!base_row.empty())
    {
        insertRow();
    }

    return Status(merged_data.pull(), true);
}

}
