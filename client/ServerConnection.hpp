#pragma once

#include <network/MessageTypes.hpp>

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/string.hpp>

#include <tbb/concurrent_unordered_map.h>
#include <tbb/queuing_rw_mutex.h>

#include <cstdint>
#include <memory>
#include <string>
#include <system_error>

namespace tell {
namespace store {

class Schema;
class SnapshotDescriptor;
class TransactionManager;

class BufferReader;

class ServerConnection : private crossbow::infinio::InfinibandSocketHandler {
public:
    class Response {
    public:
        Response()
                : mType(ResponseType::ERROR),
                  mMessage(nullptr) {
        }

        Response(ResponseType type, BufferReader* message)
                : mType(type),
                  mMessage(message) {
        }

        void reset();

        bool createTable(uint64_t& tableId, std::error_code& ec);

        bool getTableId(uint64_t& tableId, std::error_code& ec);

        bool get(size_t& size, const char*& data, uint64_t& version, bool& isNewest, std::error_code& ec);

        bool modification(std::error_code& ec);

    private:
        bool checkMessage(ResponseType type, std::error_code& ec);

        ResponseType mType;
        BufferReader* mMessage;
    };

    ServerConnection(crossbow::infinio::InfinibandService& service, TransactionManager& manager)
            : mSocket(service.createSocket()),
              mManager(manager) {
    }

    ~ServerConnection();

    void execute(std::function<void()> fun, std::error_code& ec) {
        mSocket->execute(std::move(fun), ec);
    }

    void connect(const crossbow::string& host, uint16_t port, std::error_code& ec);

    void shutdown();

    void createTable(uint64_t transactionId, const crossbow::string& name, const Schema& schema, std::error_code& ec);

    void getTableId(uint64_t transactionId, const crossbow::string& name, std::error_code& ec);

    void get(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

    void getNewest(uint64_t transactionId, uint64_t tableId, uint64_t key, std::error_code& ec);

    void update(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
            const SnapshotDescriptor& snapshot, std::error_code& ec);

    void insert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
            const SnapshotDescriptor& snapshot, bool succeeded, std::error_code& ec);

    void remove(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

    void revert(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

private:
    virtual void onConnected(const crossbow::string& data, const std::error_code& ec) final override;

    virtual void onReceive(const void* buffer, size_t length, const std::error_code& ec) final override;

    virtual void onSend(uint32_t userId, const std::error_code& ec) final override;

    virtual void onDisconnect() final override;

    virtual void onDisconnected() final override;

    void sendRequest(crossbow::infinio::InfinibandBuffer& buffer, std::error_code& ec);

    crossbow::infinio::InfinibandSocket mSocket;

    TransactionManager& mManager;
};

} // namespace store
} // namespace tell
