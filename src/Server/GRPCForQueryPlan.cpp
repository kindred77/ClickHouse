#include "GRPCForQueryPlan.h"
#include "clickhouse_grpc.grpc.pb.h"
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{
void GRPCDoTableScan([[maybe_unused]] QueryPlan & query_plan, [[maybe_unused]] GRPCTableScanStep table_scan_step)
{

}

void GRPCDoFilter([[maybe_unused]] QueryPlan & query_plan, [[maybe_unused]] GRPCFilterStep filter_step)
{

}

}

