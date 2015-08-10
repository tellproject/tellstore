#include "ServerScanQuery.hpp"

#include "ServerConfig.hpp"

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

ServerScanQuery::ServerScanQuery(uint16_t scanId, crossbow::infinio::MessageId messageId, ScanQueryType queryType,
        std::unique_ptr<char[]> selectionData, size_t selectionLength, std::unique_ptr<char[]> queryData,
        size_t queryLength, std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot, const Record& record,
        ScanBufferManager& scanBufferManager, crossbow::infinio::RemoteMemoryRegion destRegion,
        crossbow::infinio::InfinibandSocket socket)
        : ScanQuery(queryType, std::move(selectionData), selectionLength, std::move(queryData), queryLength,
                std::move(snapshot), record),
          mActive(0u),
          mScanId(scanId),
          mMessageId(messageId),
          mScanBufferManager(scanBufferManager),
          mDestRegion(std::move(destRegion)),
          mSocket(std::move(socket)),
          mOffset(0u) {
}

std::tuple<char*, uint32_t> ServerScanQuery::acquireBuffer() {
    return mScanBufferManager.acquireBuffer();
}

void ServerScanQuery::writeOngoing(const char* start, const char* end, uint16_t tupleCount, std::error_code& ec) {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);
    doWrite(start, end, tupleCount, ScanStatusIndicator::ONGOING, ec);
}

void ServerScanQuery::writeLast(const char* start, const char* end, uint16_t tupleCount, std::error_code& ec) {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);
    --mActive;

    auto status = (mActive == 0 ? ScanStatusIndicator::DONE : ScanStatusIndicator::ONGOING);
    doWrite(start, end, tupleCount, status, ec);
}

void ServerScanQuery::writeLast(std::error_code& ec) {
    typename decltype(mSendMutex)::scoped_lock _(mSendMutex);
    --mActive;

    if (mActive == 0) {
        auto userId = (static_cast<uint32_t>(mScanId) << 16) | static_cast<uint32_t>(ScanStatusIndicator::DONE);
        crossbow::infinio::ScatterGatherBuffer buffer(crossbow::infinio::InfinibandBuffer::INVALID_ID);

        while (true) {
            mSocket->write(buffer, mDestRegion, mOffset, userId, ec);
            if (ec) {
                // When we get a no memory error we overran the work queue because we are sending too fast
                if (ec == std::errc::not_enough_memory) {
                    std::this_thread::yield();
                    ec = std::error_code();
                    continue;
                }
                return;
            }
            break;
        }
    }
}

ScanQueryProcessor ServerScanQuery::createProcessor() {
    ++mActive;
    return ScanQueryProcessor(this);
}

void ServerScanQuery::doWrite(const char* start, const char* end, uint16_t tupleCount, ScanStatusIndicator status,
        std::error_code& ec) {
    LOG_ASSERT(tupleCount > 0, "Buffer containing no tuples");
    LOG_ASSERT(end > start, "Invalid buffer");

    auto immediate = (static_cast<uint32_t>(mScanId) << 16) | static_cast<uint32_t>(tupleCount);
    auto userId = (static_cast<uint32_t>(mScanId) << 16) | static_cast<uint32_t>(status);

    auto length = static_cast<uint32_t>(end - start);
    auto buffer = mScanBufferManager.getBuffer(start, length);

    while (true) {
        mSocket->write(buffer, mDestRegion, mOffset, userId, immediate, ec);
        if (ec) {
            // When we get a no memory error we overran the work queue because we are sending too fast
            if (ec == std::errc::not_enough_memory) {
                std::this_thread::yield();
                ec = std::error_code();
                continue;
            }
            return;
        }
        break;
    }
    mOffset += buffer.length();
}

} // namespace store
} // namespace tell
