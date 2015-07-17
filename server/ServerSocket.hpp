#pragma once

#include "ServerConfig.hpp"
#include "ClientScanQuery.hpp"

#include <tellstore.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/ByteBuffer.hpp>
#include <crossbow/infinio/RpcServer.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <system_error>

namespace tell {
namespace store {

class ServerManager;

/**
 * @brief Handles communication with one TellStore client
 *
 * Listens for incoming RPC requests, performs the desired action and sends the response back to the client.
 */
class ServerSocket : public crossbow::infinio::RpcServerSocket<ServerManager, ServerSocket> {
    using Base = crossbow::infinio::RpcServerSocket<ServerManager, ServerSocket>;

public:
    ServerSocket(ServerManager& manager, Storage& storage, crossbow::infinio::InfinibandProcessor& processor,
            crossbow::infinio::InfinibandSocket socket)
            : Base(manager, processor, std::move(socket), crossbow::string()),
              mStorage(storage) {
    }

private:
    friend Base;

    void onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType,
        crossbow::infinio::BufferReader& message);

    /**
     * The create table request has the following format:
     * - 2 bytes: Length of the table name string
     * - x bytes: The table name string
     * - y bytes: Variable padding to make message 8 byte aligned
     * - 4 bytes: Length of the schema field
     * - x bytes: The table schema
     *
     * The response consists of the following format:
     * - 8 bytes: The table ID of the newly created table or 0 when the table already exists
     */
    void handleCreateTable(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    /**
     * The get table ID request has the following format:
     * - 2 bytes: Length of the table name string
     * - x bytes: The table name string
     *
     * The response consists of the following format:
     * - 8 bytes: The table ID of the table or 0 when the table does not exist
     */
    void handleGetTable(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    /**
     * The get request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 8 bytes: The version of the tuple
     * - 1 byte:  Whether the tuple is the newest one
     * - 1 byte:  Whether the tuple was found
     * If the tuple was found:
     * - 2 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     */
    void handleGet(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    /**
     * The update request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - 4 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     * - y bytes: Variable padding to make message 8 byte aligned
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the update was successfull
     */
    void handleUpdate(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    /**
     * The insert request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - 1 byte:  Whether we want to know if the operation was successful or not
     * - 3 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     * - y bytes: Variable padding to make message 8 byte aligned
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the insert was successfull
     */
    void handleInsert(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    /**
     * The remove request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the remove was successfull
     */
    void handleRemove(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    /**
     * The revert request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the revert was successfull
     */
    void handleRevert(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    /**
     * The scan request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 2 bytes: The ID associated with this scan
     * - 6 bytes: Padding
     * - 8 bytes: The address of the remote memory region
     * - 8 bytes: Length of the remote memory region
     * - 4 bytes: The access key of the remote memory region
     * - 4 bytes: Length of the query buffer's data field
     * - x bytes: The query buffer's data
     * - y bytes: Variable padding to make message 8 byte aligned
     * - x bytes: Snapshot descriptor
     */
    void handleScan(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request);

    virtual void onWrite(uint32_t userId, uint16_t bufferId, const std::error_code& ec) final override;

    /**
     * @brief Get the snapshot associated with the request and pass it to the function
     *
     * The snapshot can come from different sources: If the client requested a cached snapshot the snapshot is
     * retrieved from the local cache or added to the cache in case the data was supplied in the snapshot message. The
     * snapshot has to be either in the cache or sent with the request (not both and not none of them at the same
     * time). If the client requested an uncached snapshot then the snapshot is parsed from the message without
     * considering the cache.
     *
     * In case of an error (no snapshot descriptor or conflicting data found) an error response will be written back and
     * the function will not be invoked.
     *
     * The snapshot descriptor has the following format:
     * - 8 bytes: The version of the snapshot
     * - 1 byte:  Whether we want to get / put the snapshot descriptor from / into the cache
     * - 1 byte:  Whether we sent the full descriptor
     * If the message contains a full descriptor:
     * - 2 bytes: Padding
     * - 4 bytes: Length of the descriptor
     * - x bytes: The descriptor data
     *
     * @param transactionId The transaction ID of the current message
     * @param request The request buffer to read the snapshot descriptor from
     * @param writer The response writer that will be used to sent a error message back to the client
     * @param f Function with the signature (const SnapshotDescriptor&)
     */
    template <typename Fun>
    void handleSnapshot(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& message, Fun f);

    /**
     * @brief Removes the snapshot from the cache
     */
    void removeSnapshot(uint64_t version);

    Storage& mStorage;

    // TODO Replace with google dense map (SnapshotDescriptor has no copy / default constructor)
    /// Snapshot cache mapping the version number to the snapshot descriptor
    std::unordered_map<uint64_t, SnapshotDescriptor> mSnapshots;

    /// Map from Scan ID to the shared data class associated with the scan
    /// The Connection has the ownership because we can only free this after all RDMA writes have been processed
    std::unordered_map<uint16_t, std::unique_ptr<ClientScanQueryData>> mScans;
};

class ServerManager : public crossbow::infinio::RpcServerManager<ServerManager, ServerSocket> {
    using Base = crossbow::infinio::RpcServerManager<ServerManager, ServerSocket>;

public:
    ServerManager(crossbow::infinio::InfinibandService& service, Storage& storage, const ServerConfig& config);

private:
    friend Base;
    friend class ServerSocket;

    ServerSocket* createConnection(crossbow::infinio::InfinibandSocket socket, const crossbow::string& data);

    crossbow::infinio::LocalMemoryRegion& pageRegion() {
        return mPageRegion;
    }

    Storage& mStorage;

    crossbow::infinio::LocalMemoryRegion mPageRegion;
    std::vector<std::unique_ptr<crossbow::infinio::InfinibandProcessor>> mProcessors;
};

} // namespace store
} // namespace tell
