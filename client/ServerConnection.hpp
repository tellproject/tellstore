#pragma once

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/string.hpp>
#include <crossbow/thread/condition_variable.hpp>
#include <crossbow/thread/mutex.hpp>

#include <tbb/concurrent_unordered_map.h>
#include <tbb/queuing_rw_mutex.h>

#include <boost/system/error_code.hpp>

#include <cstdint>
#include <memory>
#include <string>

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace tell {
namespace store {

namespace proto {
class RpcRequest;
class RpcResponse;
} // namespace proto

class ClientConfig;
class Schema;
class SnapshotDescriptor;

class ServerConnection : private crossbow::infinio::InfinibandSocketHandler {
public:
    ServerConnection(crossbow::infinio::InfinibandService& service)
            : mSocket(service),
              mMessageId(0x1u) {
    }

    ~ServerConnection();

    void init(const ClientConfig& config, boost::system::error_code& ec);

    void shutdown();

    bool createTable(const crossbow::string& name, const Schema& schema, uint64_t& tableId,
            boost::system::error_code& ec);

    bool get(uint64_t tableId, uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot,
            bool& isNewest, boost::system::error_code& ec);

    void insert(uint64_t tableId, uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
            boost::system::error_code& ec, bool* succeeded = nullptr);

private:
    struct ExecutionContext {
        int type;

        google::protobuf::Message** message;

        boost::system::error_code* ec;

        crossbow::condition_variable* cond;
    };

    class ExecutionContextGuard {
    public:
        ExecutionContextGuard(ServerConnection& con, uint64_t id, int type, google::protobuf::Message*& message,
                boost::system::error_code& ec);

        ~ExecutionContextGuard();

        void waitForCompletion();

    private:
        ServerConnection& mConnection;
        uint64_t mId;

        crossbow::mutex mMutex;
        crossbow::condition_variable mCond;
    };

    virtual void onConnected(const boost::system::error_code& ec) final override;

    virtual void onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) final override;

    virtual void onSend(uint32_t userId, const boost::system::error_code& ec) final override;

    virtual void onDisconnect() final override;

    virtual void onDisconnected() final override;

    void handleResponse(proto::RpcResponse& response);

    template<class Response>
    std::unique_ptr<Response> sendRequest(std::unique_ptr<proto::RpcRequest> request, int field,
            boost::system::error_code& ec);

    crossbow::infinio::InfinibandSocket mSocket;

    std::atomic<uint64_t> mMessageId;

    tbb::queuing_rw_mutex mContextsMutex;

    tbb::concurrent_unordered_map<uint64_t, ExecutionContext> mContexts;
};

} // namespace store
} // namespace tell
