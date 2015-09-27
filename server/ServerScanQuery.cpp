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
#include "ServerScanQuery.hpp"

#include "ServerConfig.hpp"
#include "ServerSocket.hpp"

#include <crossbow/logger.hpp>

#include <stdexcept>
#include <thread>

namespace tell {
namespace store {

ScanBufferManager::ScanBufferManager(crossbow::infinio::InfinibandService& service, const ServerConfig& config)
        : mScanBufferCount(config.scanBufferCount),
          mScanBufferLength(config.scanBufferLength),
          mRegion(service.allocateMemoryRegion(static_cast<size_t>(mScanBufferCount)
                * static_cast<size_t>(mScanBufferLength), IBV_ACCESS_LOCAL_WRITE)),
          mBufferStack(mScanBufferCount, crossbow::infinio::InfinibandBuffer::INVALID_ID) {
    for (decltype(mScanBufferCount) i = 0u; i < mScanBufferCount; ++i) {
        if (!mBufferStack.push(i)) {
            throw std::runtime_error("Unable to push buffer id on stack");
        }
    }
}

std::tuple<char*, uint32_t> ScanBufferManager::acquireBuffer() {
    uint16_t id;
    if (!mBufferStack.pop(id)) {
        return std::make_tuple(nullptr, 0u);
    }

    auto data = mRegion.address() + static_cast<size_t>(id) * static_cast<size_t>(mScanBufferLength);
    return std::make_tuple(reinterpret_cast<char*>(data), mScanBufferLength);
}

void ScanBufferManager::releaseBuffer(uint16_t id) {
    while (!mBufferStack.push(id));
}

crossbow::infinio::InfinibandBuffer ScanBufferManager::getBuffer(const char* data, uint32_t length) {
    auto offset = reinterpret_cast<size_t>(data - mRegion.address());
    auto id = static_cast<uint16_t>(offset / static_cast<size_t>(mScanBufferLength));
    return mRegion.acquireBuffer(id, offset, length);
}

ServerScanQuery::ServerScanQuery(uint16_t scanId, ScanQueryType queryType, std::unique_ptr<char[]> selectionData,
        size_t selectionLength, std::unique_ptr<char[]> queryData, size_t queryLength,
        std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot, const Record& record,
        ScanBufferManager& scanBufferManager, crossbow::infinio::RemoteMemoryRegion destRegion, ServerSocket& socket)
        : ScanQuery(queryType, std::move(selectionData), selectionLength, std::move(queryData), queryLength,
                std::move(snapshot), record),
          mActive(0u),
          mScanId(scanId),
          mProgressRequest(true),
          mScanBufferManager(scanBufferManager),
          mDestRegion(std::move(destRegion)),
          mSocket(socket),
          mOffset(0u) {
}

void ServerScanQuery::requestProgress(size_t offsetRead) {
    // Check if data was written since the last request otherwise queue the progress update
    // We can not block on the lock as the scan thread might loop until there is space left in the send queue but the
    // network thread is blocked on the lock and unable to process the completion queue to make space on it
    typename decltype(mSendMutex)::scoped_lock lock;
    if (lock.try_acquire(mSendMutex) && mOffset > offsetRead) {
        mSocket.writeScanProgress(mScanId, false, mOffset);
    } else {
        mProgressRequest = true;
    }
}

void ServerScanQuery::completeScan() {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);

    mSocket.writeScanProgress(mScanId, true, mOffset);
}

std::tuple<char*, uint32_t> ServerScanQuery::acquireBuffer() {
    return mScanBufferManager.acquireBuffer();
}

void ServerScanQuery::writeOngoing(const char* start, const char* end, std::error_code& ec) {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);
    doWrite(start, end, ScanStatusIndicator::ONGOING, ec);
}

void ServerScanQuery::writeLast(const char* start, const char* end, std::error_code& ec) {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);

    --mActive;
    auto status = (mActive == 0 ? ScanStatusIndicator::DONE : ScanStatusIndicator::ONGOING);
    doWrite(start, end, status, ec);
}

void ServerScanQuery::writeLast(std::error_code& ec) {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);

    --mActive;
    if (mActive == 0) {
        auto userId = (static_cast<uint32_t>(mScanId) << 16) | static_cast<uint32_t>(ScanStatusIndicator::DONE);
        crossbow::infinio::ScatterGatherBuffer buffer(crossbow::infinio::InfinibandBuffer::INVALID_ID);

        mSocket.writeScanBuffer(buffer, mDestRegion, mOffset, userId, ec);
        if (ec) {
            LOG_ERROR("Error while sending scan buffer [error = %1% %2%]", ec, ec.message());
            // TODO Error handling
            return;
        }
    }
}

ScanQueryProcessor ServerScanQuery::createProcessor() {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);
    ++mActive;
    return ScanQueryProcessor(this);
}

void ServerScanQuery::doWrite(const char* start, const char* end, ScanStatusIndicator status, std::error_code& ec) {
    LOG_ASSERT(end > start, "Invalid buffer");

    auto userId = (static_cast<uint32_t>(mScanId) << 16) | static_cast<uint32_t>(status);

    auto length = static_cast<uint32_t>(end - start);
    auto buffer = mScanBufferManager.getBuffer(start, length);

    mSocket.writeScanBuffer(buffer, mDestRegion, mOffset, userId, ec);
    if (ec) {
        LOG_ERROR("Error while sending scan buffer [error = %1% %2%]", ec, ec.message());
        // TODO Error handling
        return;
    }
    mOffset += length;

    // TODO Offset might not be accurate (requests in flight may fail)
    if (mProgressRequest) {
        mProgressRequest = false;
        auto offset = mOffset;
        mSocket.execute([this, offset] () {
            mSocket.writeScanProgress(mScanId, false, offset);
        });
    }
}

} // namespace store
} // namespace tell
