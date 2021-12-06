#include "GRPCForQueryPlan.h"
#if USE_GRPC
#include "clickhouse_grpc.grpc.pb.h"
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{
void GRPCDoTableScan(QueryPlan & query_plan, GRPCTableScanStep table_scan_step)
{

}

void GRPCDoFilter(QueryPlan & query_plan, GRPCFilterStep filter_step)
{

}

}

#endif

