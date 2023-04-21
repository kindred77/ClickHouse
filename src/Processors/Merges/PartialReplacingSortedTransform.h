#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/PartialReplacingSortedAlgorithm.h>


namespace DB
{

/// Implementation of IMergingTransform via ReplacingSortedAlgorithm.
class PartialReplacingSortedTransform final : public IMergingTransform<PartialReplacingSortedAlgorithm>
{
public:
    PartialReplacingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & part_cols_names_column,
        Names primary_key_columns_,
        Names all_column_names,
        size_t max_block_size,
        const String& delete_flag_ = "_delete_flag",
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false)
        : IMergingTransform(
            num_inputs, header, header, true,
            header,
            num_inputs,
            std::move(description_),
            part_cols_names_column,
            primary_key_columns_,
            all_column_names,
            max_block_size,
            delete_flag_,
            out_row_sources_buf_,
            use_average_block_sizes)
    {
    }

    String getName() const override { return "PartialReplacingSorted"; }
};

}
