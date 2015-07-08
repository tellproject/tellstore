#pragma once

#include "ServerConfig.hpp"
#include "ClientScanQuery.hpp"

#include <tellstore.hpp>
#include <network/ErrorCode.hpp>
#include <network/MessageTypes.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/ByteBuffer.hpp>
#include <crossbow/infinio/BatchingMessageSocket.hpp>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <string>
#include <system_error>

namespace tell {
namespace store {

class ConnectionManager;

/**
 * @brief The ClientConnection class handles communication with one client
 *
 * Listens for incoming RPC requests, performs the desired action and sends the response back to the client.
 */
class ClientConnection : private crossbow::infinio::BatchingMessageSocket<ClientConnection> {
public:
    ClientConnection(ConnectionManager& manager, Storage& storage, crossbow::infinio::InfinibandSocket socket)
            : crossbow::infinio::BatchingMessageSocket<ClientConnection>(std::move(socket)),
              mManager(manager),
              mStorage(storage) {
    }

    ~ClientConnection();

    void shutdown();

private:
    friend class crossbow::infinio::BatchingMessageSocket<ClientConnection>;

    virtual void onConnected(const crossbow::string& data, const std::error_code& ec) final override;

    void onMessage(uint64_t transactionId, uint32_t messageType, crossbow::infinio::BufferReader& message);

    void onSocketError(const std::error_code& ec);

    virtual void onWrite(uint32_t userId, uint16_t bufferId, const std::error_code& ec) final override;

    virtual void onDisconnect() final override;

    virtual void onDisconnected() final override;

    void closeConnection();

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
     * @param transactionId The transaction ID of the current message
     * @param request The request buffer to read the snapshot descriptor from
     * @param writer The response writer that will be used to sent a error message back to the client
     * @param f Function with the signature (const SnapshotDescriptor&)
     */
    template <typename Fun>
    void handleSnapshot(uint64_t transactionId, crossbow::infinio::BufferReader& request, Fun f);

    /**
     * @brief Removes the snapshot from the cache
     */
    void removeSnapshot(uint64_t version);

    crossbow::infinio::BufferWriter writeResponse(uint64_t transactionId, ResponseType response, size_t length,
            std::error_code& ec) {
        return writeMessage(transactionId, static_cast<uint32_t>(response), length, ec);
    }

    /**
     * @brief Writes the error response back to the client
     *
     * @param transactionId The target transaction ID
     * @param error Error code to send
     */
    void writeErrorResponse(uint64_t transactionId, error::server_errors error);

    ConnectionManager& mManager;
    Storage& mStorage;

    // TODO Replace with google dense map (SnapshotDescriptor has no copy / default constructor)
    /// Snapshot cache mapping the version number to the snapshot descriptor
    std::unordered_map<uint64_t, SnapshotDescriptor> mSnapshots;

    /// Map from Scan ID to the shared data class associated with the scan
    /// The Connection has the ownership because we can only free this after all RDMA writes have been processed
    std::unordered_map<uint16_t, std::unique_ptr<ClientScanQueryData>> mScans;
};

} // namespace store
} // namespace tell
