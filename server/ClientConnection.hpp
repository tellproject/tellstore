#pragma once

#include "ServerConfig.hpp"

#include <tellstore.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>

#include <tbb/queuing_rw_mutex.h>
#include <tbb/concurrent_unordered_map.h>

#include <boost/system/error_code.hpp>

#include <cstdint>
#include <string>

namespace tell {
namespace store {

namespace proto {
class RpcRequest;
class RpcResponse;
class RpcResponseBatch;
class SnapshotDescriptor;
} // namespace proto

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

    void handleRequest(const proto::RpcRequest& request, proto::RpcResponse& response);

    void sendResponseBatch(const proto::RpcResponseBatch& responseBatch);

    /**
     * @brief Get the snapshot associated with the request and pass it to the function
     *
     * The snapshot can come from different sources: If the client requested a cached snapshot the snapshot is
     * retrieved from the local cache or added to the cache in case the data was supplied in the snapshot message. The
     * snapshot has to be either in the cache or sent with the request (not both and not none of them at the same
     * time). If the client requested an uncached snapshot then the snapshot is parsed from the message without
     * considering the cache.
     *
     * In case of an error (no snapshot descriptor or conflicting data found) the error response will be set and the
     * function will not be invoked.
     *
     * @param snapshot Snapshot message the client sent
     * @param response The response that will be sent back to the client
     * @param f Function with the signature (const SnapshotDescriptor&, proto::RpcResponse&)
     */
    template <typename Fun>
    void handleSnapshot(const proto::SnapshotDescriptor& snapshot, proto::RpcResponse& response, Fun f);

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
