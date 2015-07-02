#pragma once

#include <network/MessageSocket.hpp>
#include <network/MessageTypes.hpp>
#include <util/GenericTuple.hpp>

#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <system_error>

namespace tell {
namespace store {

class Record;
class Schema;
class SnapshotDescriptor;
class TransactionProcessor;

class ServerConnection : private MessageSocket<ServerConnection> {
public:
    class Response {
    public:
        Response()
                : mType(ResponseType::UNKOWN),
                  mMessage(nullptr) {
        }

        Response(ResponseType type, BufferReader* message)
                : mType(type),
                  mMessage(message) {
        }

        void reset();

        bool isSet() {
            return (mMessage != nullptr);
        }

        bool createTable(uint64_t& tableId, std::error_code& ec);

        bool getTableId(uint64_t& tableId, std::error_code& ec);

        bool get(size_t& size, const char*& data, uint64_t& version, bool& isNewest, std::error_code& ec);

        bool modification(std::error_code& ec);

        void scan(uint16_t& scanId, std::error_code& ec);

    private:
        bool checkMessage(ResponseType type, std::error_code& ec);

        ResponseType mType;
        BufferReader* mMessage;
    };

    ServerConnection(crossbow::infinio::InfinibandSocket socket, TransactionProcessor& processor)
            : MessageSocket(std::move(socket)),
              mProcessor(processor) {
    }

    ~ServerConnection();

    void execute(std::function<void()> fun, std::error_code& ec) {
        mSocket->execute(std::move(fun), ec);
    }

    void connect(const crossbow::string& host, uint16_t port, uint64_t thread);

    void shutdown();

    void createTable(uint64_t transactionId, const crossbow::string& name, const Schema& schema, std::error_code& ec);

    void getTableId(uint64_t transactionId, const crossbow::string& name, std::error_code& ec);

    void get(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

    void getNewest(uint64_t transactionId, uint64_t tableId, uint64_t key, std::error_code& ec);

    void update(uint64_t transactionId, uint64_t tableId, uint64_t key, const Record& record, const GenericTuple& tuple,
            const SnapshotDescriptor& snapshot, std::error_code& ec);

    void update(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
            const SnapshotDescriptor& snapshot, std::error_code& ec);

    void insert(uint64_t transactionId, uint64_t tableId, uint64_t key, const Record& record, const GenericTuple& tuple,
            const SnapshotDescriptor& snapshot, bool succeeded, std::error_code& ec);

    void insert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
            const SnapshotDescriptor& snapshot, bool succeeded, std::error_code& ec);

    void remove(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

    void revert(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

    void scan(uint64_t transactionId, uint64_t tableId, uint16_t id,
            const crossbow::infinio::LocalMemoryRegion& destRegion, uint32_t querySize, const char* query,
            const SnapshotDescriptor& snapshot, std::error_code& ec);

private:
    friend class MessageSocket;

    template <typename Fun>
    void doUpdate(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size,
            const SnapshotDescriptor& snapshot, Fun f, std::error_code& ec);

    template <typename Fun>
    void doInsert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size,
            const SnapshotDescriptor& snapshot, bool succeeded, Fun f, std::error_code& ec);

    virtual void onConnected(const crossbow::string& data, const std::error_code& ec) final override;

    void onMessage(uint64_t transactionId, uint32_t messageType, BufferReader message);

    void onSocketError(const std::error_code& ec);

    virtual void onImmediate(uint32_t data) final override;

    virtual void onDisconnect() final override;

    virtual void onDisconnected() final override;

    BufferWriter writeRequest(uint64_t transactionId, RequestType request, size_t length, std::error_code& ec) {
        return writeMessage(transactionId, static_cast<uint32_t>(request), length, ec);
    }

    TransactionProcessor& mProcessor;
};

} // namespace store
} // namespace tell
