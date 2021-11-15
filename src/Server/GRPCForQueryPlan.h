#pragma once

#include "clickhouse_grpc.grpc.pb.h"
#include <Processors/QueryPlan/QueryPlan.h>
#include <Server/GRPCServer.h>

namespace DB
{
void GRPCDoTableScan(QueryPlan & query_plan, GRPCTableScanStep table_scan_step);

void GRPCDoFilter(QueryPlan & query_plan, GRPCFilterStep filter_step);

}
