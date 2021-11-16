#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_GRPC
#include <Poco/Net/SocketAddress.h>
#include "clickhouse_grpc.grpc.pb.h"

using GRPCService = clickhouse::grpc::ClickHouseService::AsyncService;
using GRPCQueryInfo = clickhouse::grpc::QueryInfo;
using GRPCResult = clickhouse::grpc::Result;
using GRPCException = clickhouse::grpc::Exception;
using GRPCProgress = clickhouse::grpc::Progress;
using GRPCQueryPlan = clickhouse::grpc::QueryPlan;
using GRPCStep = clickhouse::grpc::Step;
using GRPCStepType = clickhouse::grpc::StepType;
using GRPCTableScanStep = clickhouse::grpc::TableScanStep;
using GRPCFilterStep = clickhouse::grpc::FilterStep;

namespace Poco { class Logger; }

namespace grpc
{
class Server;
class ServerCompletionQueue;
}

namespace DB
{
class IServer;

namespace
{
using CompletionCallback = std::function<void(bool)>;

/// Requests a connection and provides low-level interface for reading and writing.
class BaseResponder
{
public:
    virtual ~BaseResponder() = default;

    virtual void start(GRPCService & grpc_service,
                       grpc::ServerCompletionQueue & new_call_queue,
                       grpc::ServerCompletionQueue & notification_queue,
                       const CompletionCallback & callback) = 0;

    virtual void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback) = 0;
    virtual void readQueryPlan(GRPCQueryPlan & query_plan_, const CompletionCallback & callback) = 0;
    virtual void write(const GRPCResult & result, const CompletionCallback & callback) = 0;
    virtual void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback) = 0;

    Poco::Net::SocketAddress getClientAddress() const { std::string peer = grpc_context.peer(); return Poco::Net::SocketAddress{peer.substr(peer.find(':') + 1)}; }

protected:
    CompletionCallback * getCallbackPtr(const CompletionCallback & callback)
    {
        /// It would be better to pass callbacks to gRPC calls.
        /// However gRPC calls can be tagged with `void *` tags only.
        /// The map `callbacks` here is used to keep callbacks until they're called.
        std::lock_guard lock{mutex};
        size_t callback_id = next_callback_id++;
        auto & callback_in_map = callbacks[callback_id];
        callback_in_map = [this, callback, callback_id](bool ok)
        {
            CompletionCallback callback_to_call;
            {
                std::lock_guard lock2{mutex};
                callback_to_call = callback;
                callbacks.erase(callback_id);
            }
            callback_to_call(ok);
        };
        return &callback_in_map;
    }

    grpc::ServerContext grpc_context;

private:
    grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> reader_writer{&grpc_context};
    grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryPlan> reader_writer_query_plan{&grpc_context};
    std::unordered_map<size_t, CompletionCallback> callbacks;
    size_t next_callback_id = 0;
    std::mutex mutex;
};

enum CallType
{
    CALL_SIMPLE,             /// ExecuteQuery() call
    CALL_WITH_STREAM_INPUT,  /// ExecuteQueryWithStreamInput() call
    CALL_WITH_STREAM_OUTPUT, /// ExecuteQueryWithStreamOutput() call
    CALL_WITH_STREAM_IO,     /// ExecuteQueryWithStreamIO() call
    CALL_QUERYPLAN,          /// ExecuteQueryPlan() call
    CALL_MAX,
};

template <enum CallType call_type>
class Responder;
}

class GRPCServer
{
public:
    GRPCServer(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_);
    ~GRPCServer();

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    void start();

    /// Stops the server. No new connections will be accepted.
    void stop();

    /// Returns the number of currently handled connections.
    size_t currentConnections() const;

    /// Returns the number of current threads.
    size_t currentThreads() const { return currentConnections(); }

private:
    using GRPCService = clickhouse::grpc::ClickHouseService::AsyncService;
    class Runner;

    IServer & iserver;
    const Poco::Net::SocketAddress address_to_listen;
    Poco::Logger * log;
    GRPCService grpc_service;
    std::unique_ptr<grpc::Server> grpc_server;
    std::unique_ptr<grpc::ServerCompletionQueue> queue;
    std::unique_ptr<Runner> runner;
};
}
#endif
