#pragma once

#include "ServerConfig.hpp"

#include <tellstore.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/InfinibandSocket.hpp>

#include <tbb/queuing_rw_mutex.h>
#include <tbb/concurrent_unordered_map.h>

#include <boost/system/error_code.hpp>

#include <cstdint>
#include <string>

namespace tell {
namespace store {

class MessageWriter;
class BufferReader;
class ConnectionManager;

/**
 * @brief The ClientConnection class handles communication with one client
 *
 * Listens for incoming RPC requests, performs the desired action and sends the response back to the client.
 */
class ClientConnection : private crossbow::infinio::InfinibandSocketHandler {
public:
    ClientConnection(ConnectionManager& manager, Storage& storage, crossbow::infinio::InfinibandSocket&& socket)
            : mManager(manager),
              mStorage(storage),
              mSocket(std::move(socket)) {
    }

    ~ClientConnection();

    void init();

    void shutdown();

private:
    virtual void onConnected(const boost::system::error_code& ec) final override;

    virtual void onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) final override;

    virtual void onSend(uint32_t userId, const boost::system::error_code& ec) final override;

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
    void handleSnapshot(uint64_t transactionId, BufferReader& request, MessageWriter& writer, Fun f);

    /**
     * @brief Removes the snapshot from the cache
     */
    void removeSnapshot(uint64_t version);

    ConnectionManager& mManager;
    Storage& mStorage;

    /// Client socket
    crossbow::infinio::InfinibandSocket mSocket;

    /// Mutex for the snapshot cache
    tbb::queuing_rw_mutex mSnapshotsMutex;

    /// Snapshot cache mapping the version number to the snapshot descriptor
    tbb::concurrent_unordered_map<uint64_t, SnapshotDescriptor> mSnapshots;

};

} // namespace store
} // namespace tell
