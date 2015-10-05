/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include "ServerConfig.hpp"
#include "ServerScanQuery.hpp"
#include "Storage.hpp"

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/infinio/RpcServer.hpp>
#include <crossbow/string.hpp>

#include <atomic>
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
            crossbow::infinio::InfinibandSocket socket, size_t maxBatchSize, uint64_t maxInflightScanBuffer)
            : Base(manager, processor, std::move(socket), crossbow::string(), maxBatchSize),
              mStorage(storage),
              mMaxInflightScanBuffer(maxInflightScanBuffer),
              mInflightScanBuffer(0u) {
    }

    /**
     * @brief Execute the function in the event loop
     */
    template <typename Fun>
    void execute(Fun fun) {
        mSocket->processor()->execute(std::move(fun));
    }

    /**
     * @brief Writes the buffer into the scan destination region
     */
    template <typename Buffer>
    void writeScanBuffer(Buffer& buffer, crossbow::infinio::RemoteMemoryRegion& destRegion, size_t offset, uint32_t userId, std::error_code& ec) {
        // Wait in case the network is overloaded
        // For performance reasons this is not really thread safe but as the number of threads accessing this variable
        // is bounded and small (2-4) the actual limit will not be exceeded by much.
        while (mInflightScanBuffer >= mMaxInflightScanBuffer) {
            std::this_thread::yield();
        }
        mInflightScanBuffer += 1u;

        mSocket->write(buffer, destRegion, offset, userId, ec);
    }

    /**
     * @brief Notifies the client of the scan progress
     *
     * Must only be called from within the socket's processing thread.
     *
     * @param scanId ID associated with the scan
     * @param done Whether the scan has completed
     * @param offset Amount of data written into the scan destination region
     */
    void writeScanProgress(uint16_t scanId, bool done, size_t offset);

private:
    friend Base;

    void onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType, crossbow::buffer_reader& message);

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
    void handleCreateTable(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

    /**
     * The get table ID request has the following format:
     * - 2 bytes: Length of the table name string
     * - x bytes: The table name string
     *
     * The response consists of the following format:
     * - 8 bytes: The table ID of the table or 0 when the table does not exist
     */
    void handleGetTable(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

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
    void handleGet(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

    /**
     * The update request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - 4 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the update was successful
     */
    void handleUpdate(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

    /**
     * The insert request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - 4 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the insert was successful
     */
    void handleInsert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

    /**
     * The remove request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the remove was successful
     */
    void handleRemove(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

    /**
     * The revert request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the revert was successful
     */
    void handleRevert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

    /**
     * The scan request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 1 byte:  The type of the query data
     * - 7 bytes: Padding
     * - 8 bytes: The address of the remote memory region
     * - 8 bytes: Length of the remote memory region
     * - 4 bytes: The access key of the remote memory region
     * - 4 bytes: Length of the selection's data field
     * - x bytes: The selection's data (8 byte aligned)
     * - 4 bytes: Padding
     * - 4 bytes: Length of the  query's data field
     * - x bytes: The query's data
     * - y bytes: Variable padding to make message 8 byte aligned
     * - x bytes: Snapshot descriptor
     */
    void handleScan(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

    /**
     * The scan progress request has the following format:
     * - 8 bytes: Offset the client read up to
     */
    void handleScanProgress(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request);

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
     * - 1 byte:  Whether we want to get / put the snapshot descriptor from / into the cache
     * - 1 byte:  Whether we sent the full descriptor
     * If the message contains a full descriptor:
     * - 6 bytes: Padding
     * - x bytes: The descriptor data
     *
     * @param transactionId The transaction ID of the current message
     * @param request The request buffer to read the snapshot descriptor from
     * @param writer The response writer that will be used to sent a error message back to the client
     * @param f Function with the signature (const SnapshotDescriptor&)
     */
    template <typename Fun>
    void handleSnapshot(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& message, Fun f);

    /**
     * @brief Removes the snapshot from the cache
     */
    void removeSnapshot(uint64_t version);

    Storage& mStorage;

    /// Maximum number of scan buffers that are in flight on the socket at the same time
    uint64_t mMaxInflightScanBuffer;

    /// Current number of scan buffers that are in flight
    std::atomic<uint64_t> mInflightScanBuffer;

    // TODO Replace with google dense map (SnapshotDescriptor has no copy / default constructor)
    /// Snapshot cache mapping the version number to the snapshot descriptor
    std::unordered_map<uint64_t, std::unique_ptr<commitmanager::SnapshotDescriptor>> mSnapshots;

    /// Map from Scan ID to the shared data class associated with the scan
    /// The Connection has the ownership because we can only free this after all RDMA writes have been processed
    std::unordered_map<uint16_t, std::unique_ptr<ServerScanQuery>> mScans;
};

class ServerManager : public crossbow::infinio::RpcServerManager<ServerManager, ServerSocket> {
    using Base = crossbow::infinio::RpcServerManager<ServerManager, ServerSocket>;

public:
    ServerManager(crossbow::infinio::InfinibandService& service, Storage& storage, const ServerConfig& config);

private:
    friend Base;
    friend class ServerSocket;

    ServerSocket* createConnection(crossbow::infinio::InfinibandSocket socket, const crossbow::string& data);

    ScanBufferManager& scanBufferManager() {
        return mScanBufferManager;
    }

    Storage& mStorage;

    size_t mMaxBatchSize;

    ScanBufferManager mScanBufferManager;
    uint64_t mMaxInflightScanBuffer;

    std::vector<std::unique_ptr<crossbow::infinio::InfinibandProcessor>> mProcessors;
};

} // namespace store
} // namespace tell
