#pragma once

#include "clickhouse_grpc.grpc.pb.h"
#include <Processors/QueryPlan/QueryPlan.h>
#include <Server/GRPCServer.h>

namespace DB
{
void doTableScanForGRPC(ContextMutablePtr& query_context, QueryPlan & query_plan, GRPCTableScanStep table_scan_step);

void doFilterForGRPC(ContextMutablePtr& query_context, QueryPlan & query_plan, GRPCFilterStep filter_step);

void testExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log, std::string query_text);

void testConstExpressionActions(ContextMutablePtr& query_context, Poco::Logger * log);

Block createTestBlock();

void dumpBlock(Poco::Logger * log, Block block);

}
