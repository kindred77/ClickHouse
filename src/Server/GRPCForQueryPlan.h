#pragma once

#include "clickhouse_grpc.grpc.pb.h"
#include <Processors/QueryPlan/QueryPlan.h>
#include <Server/GRPCServer.h>

namespace DB
{
void GRPCDoTableScan(ContextMutablePtr& query_context, QueryPlan & query_plan, GRPCTableScanStep table_scan_step);

void GRPCDoFilter(ContextMutablePtr& query_context, QueryPlan & query_plan, GRPCFilterStep filter_step);

void TestExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log, std::string query_text);

void TestConstExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log);

Block CreateTestBlock();

void DumpBlock(Poco::Logger * log, Block block);

}
