#pragma once

#include <util/MessageTypes.hpp>

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/string.hpp>

#include <tbb/concurrent_unordered_map.h>
#include <tbb/queuing_rw_mutex.h>

#include <boost/system/error_code.hpp>

#include <cstdint>
#include <memory>
#include <string>

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

        bool createTable(uint64_t& tableId, boost::system::error_code& ec);

        bool getTableId(uint64_t& tableId, boost::system::error_code& ec);

        bool get(size_t& size, const char*& data, uint64_t& version, bool& isNewest, boost::system::error_code& ec);

        bool modification(boost::system::error_code& ec);

    private:
        bool checkMessage(ResponseType type, boost::system::error_code& ec);

        ResponseType mType;
        BufferReader* mMessage;
    };

    ServerConnection(crossbow::infinio::InfinibandService& service, TransactionManager& manager)
            : mSocket(service),
              mManager(manager) {
    }

    ~ServerConnection();

    void connect(const crossbow::string& host, uint16_t port, boost::system::error_code& ec);

    void shutdown();

    void createTable(uint64_t transactionId, const crossbow::string& name, const Schema& schema,
            boost::system::error_code& ec);

    void getTableId(uint64_t transactionId, const crossbow::string& name, boost::system::error_code& ec);

    void get(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            boost::system::error_code& ec);

    void getNewest(uint64_t transactionId, uint64_t tableId, uint64_t key, boost::system::error_code& ec);

    void update(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
            const SnapshotDescriptor& snapshot, boost::system::error_code& ec);

    void insert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
            const SnapshotDescriptor& snapshot, bool succeeded, boost::system::error_code& ec);

    void remove(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            boost::system::error_code& ec);

    void revert(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            boost::system::error_code& ec);

private:
    virtual void onConnected(const boost::system::error_code& ec) final override;

    virtual void onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) final override;

    virtual void onSend(uint32_t userId, const boost::system::error_code& ec) final override;

    virtual void onDisconnect() final override;

    virtual void onDisconnected() final override;

    void sendRequest(crossbow::infinio::InfinibandBuffer& buffer, boost::system::error_code& ec);

    crossbow::infinio::InfinibandSocket mSocket;

    TransactionManager& mManager;
};

} // namespace store
} // namespace tell
