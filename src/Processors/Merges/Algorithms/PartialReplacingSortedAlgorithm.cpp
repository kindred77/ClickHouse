#include <Processors/Merges/Algorithms/PartialReplacingSortedAlgorithm.h>

namespace DB
{

void PartialReplacingSortedAlgorithm::PartialReplacingMergedData::insertRowWithPartial(const ColumnRawPtrs & raw_columns,
        const std::vector<size_t>& rows,
        size_t part_replacing_colnames_col_idx,
        const UniqueString& merged_unique_colnames,
        size_t block_size)
{
    size_t num_columns = raw_columns.size();
    for (size_t i = 0; i < num_columns; ++i)
    {
        //insert value of partial repacling colunm
        if (!merged_unique_colnames.empty() && part_replacing_colnames_col_idx == i)
        {
            const Field & res = Array(merged_unique_colnames.begin(), merged_unique_colnames.end());
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
        const Block & header_, size_t num_inputs,
        SortDescription description_, const String & part_replacing_colnames_colname_,
        Names primary_key_columns_,
        Names all_column_names,
        size_t max_block_size,
        const String& delete_flag_,
        WriteBuffer * out_row_sources_buf_,
        bool use_average_block_sizes)
        : IMergingAlgorithmWithSharedChunks(num_inputs, std::move(description_), out_row_sources_buf_, max_row_refs)
        , header(header_)
        , delete_flag(delete_flag_)
        , merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
{
    for (auto & column_name : primary_key_columns_)
    {
        primary_keys_pos.push_back(header.getPositionByName(column_name));
    }

    part_replacing_col_info = std::make_tuple(part_replacing_colnames_colname_,
        header.getPositionByName(part_replacing_colnames_colname_));
}

void PartialReplacingSortedAlgorithm::insertRow()
{
    if (if_partial_replaced)
    {
        merged_data.insertRowWithPartial(all_columns, row_nums, std::get<1>(part_replacing_col_info), merged_unique_colnames, owned_chunk_row_num);
    }
    else
    {
        merged_data.insertRow(*base_row.all_columns, base_row.row_num, owned_chunk_row_num);
    }
    clear();
}

bool PartialReplacingSortedAlgorithm::hasEqualPrimaryKeyColumnsWithBaseRow(SortCursor current)
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

void PartialReplacingSortedAlgorithm::extractReplacingColunmNames(ReplacingNames& replacing_names, const IColumn* column_ptr, size_t row)
{
    replacing_names.clear();
    if (const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(column_ptr))
    {
        const auto len = column_array->getOffsets()[row] - column_array->getOffsets()[row - 1];
        if (len < 1)
        {
            return;
        }
        const ColumnLowCardinality * col_lowcard = checkAndGetColumn<ColumnLowCardinality>(column_array->getData());
        auto offset = column_array->getOffsets()[row - 1];
        for (size_t i = 0; i < len; ++i)
        {
            replacing_names.emplace_back(col_lowcard->getDataAt(offset + i).toString());
        }
    }

    return;
}

void PartialReplacingSortedAlgorithm::replacePartially(const ReplacingNames& current_colnames, SortCursor current)
{
    bool base_row_is_partial = !col_names_base_row.empty();
    for (auto col_name : current_colnames)
    {
        if (invalid(col_name))
            continue;
        //if base row is a partial columns row, merge all the column names into base row
        if (base_row_is_partial)
        {
            //initialize
            if (merged_unique_colnames.empty())
            {
                for (auto colname_in_base : col_names_base_row)
                {
                    if (invalid(colname_in_base))
                        continue;
                    merged_unique_colnames.insert(colname_in_base);
                }
            }
            merged_unique_colnames.insert(col_name);
        }
        //entering partial column merging mode
        if (all_columns.empty())
        {
            all_columns = {base_row.all_columns->begin(), base_row.all_columns->end()};
            row_nums.assign(all_columns.size(), base_row.row_num);
        }
        if_partial_replaced = true;
        //position in block/chunk
        if (!header.has(col_name)) continue;
        size_t pos = header.getPositionByName(col_name);
        //replace partially
        all_columns[pos] = current->all_columns[pos];
        row_nums[pos] = current->getRow();
    }
}

IMergingAlgorithm::Status PartialReplacingSortedAlgorithm::merge()
{
    ReplacingNames col_names;
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

        extractReplacingColunmNames(col_names, current->all_columns[std::get<1>(part_replacing_col_info)], current->getRow());
        const auto del_flag = col_names.size() == 1 && col_names[0] == delete_flag;

        if (base_row.empty())
        {
            setBaseRow(current, col_names);
            //we pass the delete flag directly when there's no base row
            //in case of merging on fly
            if (del_flag)
            {
                insertRow();
            }
        }
        else
        {
            //partial columns row, replace base row according to partial column names in array.
            if (!col_names.empty())
            {
                //delete
                if (del_flag)
                {
                    clear();
                }
                else
                {
                    replacePartially(col_names, current);
                }
            }
            //full columns row, set it as a base row
            else
            {
                setBaseRow(current, col_names);
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
