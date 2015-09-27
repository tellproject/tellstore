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

#include <util/ScanQuery.hpp>

#include <crossbow/fixed_size_stack.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/infinio/MessageId.hpp>
#include <crossbow/non_copyable.hpp>

#include <tbb/spin_mutex.h>

#include <cstdint>
#include <system_error>
#include <tuple>

namespace tell {
namespace store {

struct ServerConfig;
class ServerSocket;

/**
 * @brief Status of the scan
 *
 * This value will be encoded with the RDMA writes as user ID so the ServerSocket knows if this was the last RDMA write
 * from a ServerScanQuery
 */
enum class ScanStatusIndicator : uint16_t {
    /// The scan is still ongoing
    ONGOING = 0x1u,

    /// The scan has processed all tuples and can be marked as done
    DONE,
};

/**
 * @brief Buffer pool for sharing scan buffers between all scans
 */
class ScanBufferManager : crossbow::non_copyable, crossbow::non_movable {
public:
    ScanBufferManager(crossbow::infinio::InfinibandService& service, const ServerConfig& config);

    /**
     * @brief Acquires a new buffer from the pool
     */
    std::tuple<char*, uint32_t> acquireBuffer();

    /**
     * @brief Release a buffer to the pool
     */
    void releaseBuffer(uint16_t id);

    /**
     * @brief Get the InfinibandBuffer associated with the data pointer
     */
    crossbow::infinio::InfinibandBuffer getBuffer(const char* data, uint32_t length);

private:
    uint16_t mScanBufferCount;

    uint32_t mScanBufferLength;

    crossbow::infinio::AllocatedMemoryRegion mRegion;

    crossbow::fixed_size_stack<uint16_t> mBufferStack;
};

/**
 * @brief ScanQuery implementation sending the scan data over the network
 */
class ServerScanQuery final : public ScanQuery {
public:
    ServerScanQuery(uint16_t scanId, ScanQueryType queryType, std::unique_ptr<char[]> selectionData,
            size_t selectionLength, std::unique_ptr<char[]> queryData, size_t queryLength,
            std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot, const Record& record,
            ScanBufferManager& scanBufferManager, crossbow::infinio::RemoteMemoryRegion destRegion,
            ServerSocket& socket);

    /**
     * @brief Request a progress update from the client
     *
     * Must be called from the socket's processing thread.
     *
     * @param offsetRead The amount of data the client already received
     */
    void requestProgress(size_t offsetRead);

    /**
     * @brief The scan completed and all in-flight packages have been received by the client
     *
     * Must be called from the socket's processing thread.
     */
    void completeScan();

    /**
     * @brief Acquires a new buffer from the pool
     */
    virtual std::tuple<char*, uint32_t> acquireBuffer() final override;

    /**
     * @brief Flushes the tuples in the buffer to the client
     *
     * This does not mark the scan processor as done.
     *
     * @param start Begin pointer to the buffer containing the tuples
     * @param end End pointer to the buffer containing the tuples
     * @param ec Error in case the write fails
     */
    virtual void writeOngoing(const char* start, const char* end, std::error_code& ec) final
            override;

    /**
     * @brief Writes the last tuples to the client
     *
     * The scan processor is marked as done and the number of active ScanQueryProcessor referencing the shared data is
     * decreased.
     *
     * @param start Begin pointer to the buffer containing the tuples
     * @param end End pointer to the buffer containing the tuples
     * @param ec Error in case the write fails
     */
    virtual void writeLast(const char* start, const char* end, std::error_code& ec) final override;

    /**
     * @brief Writes the last tuples to the client
     *
     * The scan processor is marked as done and the number of active ScanQueryProcessor referencing the shared data is
     * decreased.
     *
     * @param ec Error in case the write fails
     */
    virtual void writeLast(std::error_code& ec) final override;

    /**
     * @brief Create a new ScanQueryProcessor associated with this scan
     *
     * Increases the number of active ScanQueryProcessor referencing the shared data.
     */
    virtual ScanQueryProcessor createProcessor() final override;

private:
    /**
     * @brief Writes the buffer to the client
     *
     * @param start Begin pointer to the buffer containing the tuples
     * @param end End pointer to the buffer containing the tuples
     * @param status Indicator if the scan is still progressing
     * @param ec Error in case the write fails
     */
    void doWrite(const char* start, const char* end, ScanStatusIndicator status, std::error_code& ec);

    /// Number of currently active ScanQueryProcessor
    uint32_t mActive;

    /// Scan ID of the starting process on the remote host
    uint16_t mScanId;

    /// Whether the client requested a progress update
    std::atomic<bool> mProgressRequest;

    /// Buffer pool to acquire scan buffer from
    ScanBufferManager& mScanBufferManager;

    /// Memory region on the remote host
    crossbow::infinio::RemoteMemoryRegion mDestRegion;

    /// Connection to the remote host
    ServerSocket& mSocket;

    /// Mutex used to serialize writes over the connection
    tbb::spin_mutex mSendMutex;

    /// Current offset into the destination region
    size_t mOffset;
};

} // namespace store
} // namespace tell
