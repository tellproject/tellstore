#pragma once

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/string.hpp>

#include <google/protobuf/message.h>

#include <tbb/concurrent_unordered_map.h>
#include <tbb/queuing_rw_mutex.h>

#include <boost/system/error_code.hpp>

#include <cstdint>
#include <memory>
#include <string>

namespace tell {
namespace store {

namespace proto {
class RpcRequest;
class RpcResponse;
} // namespace proto

class Schema;
class SnapshotDescriptor;
class TransactionManager;

class ServerConnection : private crossbow::infinio::InfinibandSocketHandler {
public:
    class Response {
    public:
        Response()
                : mError(0x0u),
                  mType(0x0u) {
        }

        Response(uint32_t error)
                : mError(error),
                  mType(0x0u) {
        }

        Response(uint32_t type, google::protobuf::Message* message)
                : mError(0x0u),
                  mType(type),
                  mMessage(message) {
        }

        void reset();

        bool createTable(uint64_t& tableId, boost::system::error_code& ec);

        bool get(size_t& size, const char*& data, bool& isNewest, boost::system::error_code& ec);

        void insert(bool* succeeded, boost::system::error_code& ec);

    private:
        template <typename R>
        R* getMessage(int field, boost::system::error_code& ec);

        uint32_t mError;
        uint32_t mType;
        std::unique_ptr<google::protobuf::Message> mMessage;
    };

    static constexpr uint64_t RESPONSE_ERROR_TYPE = std::numeric_limits<std::uint64_t>::max();

    ServerConnection(crossbow::infinio::InfinibandService& service, TransactionManager& manager)
            : mSocket(service),
              mManager(manager) {
    }

    ~ServerConnection();

    void connect(const crossbow::string& host, uint16_t port, boost::system::error_code& ec);

    void shutdown();

    void createTable(uint64_t transactionId, const crossbow::string& name, const Schema& schema,
            boost::system::error_code& ec);

    void get(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            boost::system::error_code& ec);

    void insert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
            const SnapshotDescriptor& snapshot, bool succeeded, boost::system::error_code& ec);

private:
    virtual void onConnected(const boost::system::error_code& ec) final override;

    virtual void onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) final override;

    virtual void onSend(uint32_t userId, const boost::system::error_code& ec) final override;

    virtual void onDisconnect() final override;

    virtual void onDisconnected() final override;

    void handleResponse(proto::RpcResponse& response);

    void sendRequest(std::unique_ptr<proto::RpcRequest> request, boost::system::error_code& ec);

    crossbow::infinio::InfinibandSocket mSocket;

    TransactionManager& mManager;
};

} // namespace store
} // namespace tell
