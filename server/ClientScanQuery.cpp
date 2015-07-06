#include "ClientScanQuery.hpp"

#include <crossbow/logger.hpp>

#include <thread>

namespace tell {
namespace store {

namespace {

/**
 * @brief Maximum number of tuples to send with a unsignaled write before issueing a signaled write
 */
const size_t gMaxUnsignaled = 320;

/**
 * @brief Maximum number of tuples per scatter/gather buffer before flushing the buffer
 */
const size_t gMaxBufferTuple = 32;

} // anonymous namespace

bool ClientScanQueryData::checkTuple(uint64_t validFrom, uint64_t validTo) const {
    return (mSnapshot.inReadSet(validFrom) && !mSnapshot.inReadSet(validTo));
}

void ClientScanQueryData::write(crossbow::infinio::ScatterGatherBuffer& buffer, std::error_code& ec) {
    tbb::spin_mutex::scoped_lock _(mSendMutex);

    if ((mUnsignaledCount + buffer.count()) >= gMaxUnsignaled) {
        writeSignaled(buffer, ScanStatus::ONGOING, ec);
    } else {
        writeUnsignaled(buffer, ec);
    }
}

void ClientScanQueryData::done(crossbow::infinio::ScatterGatherBuffer& buffer, std::error_code& ec) {
    tbb::spin_mutex::scoped_lock _(mSendMutex);
    writeSignaled(buffer, ScanStatus::DONE, ec);
}

void ClientScanQueryData::writeUnsignaled(crossbow::infinio::ScatterGatherBuffer& buffer, std::error_code& ec) {
    while (true) {
        mSocket->writeUnsignaled(buffer, mDestRegion, mOffset, to_underlying(ScanStatus::ONGOING), ec);
        if (ec) {
            // When we get a no memory error we overran the work queue because we are sending too fast
            if (ec.value() == ENOMEM && ec.category() == std::system_category()) {
                std::this_thread::yield();
                ec = std::error_code();
                continue;
            }
            return;
        }
        break;
    }
    mUnsignaledCount += buffer.count();
    mOffset += buffer.length();
}

void ClientScanQueryData::writeSignaled(crossbow::infinio::ScatterGatherBuffer& buffer, ScanStatus status,
        std::error_code& ec) {
    uint32_t data = (static_cast<uint32_t>(buffer.id()) << 16)
            | static_cast<uint32_t>(mUnsignaledCount + buffer.count());
    while (true) {
        mSocket->write(buffer, mDestRegion, mOffset, to_underlying(status), data, ec);
        if (ec) {
            // When we get a no memory error we overran the work queue because we are sending too fast
            if (ec.value() == ENOMEM && ec.category() == std::system_category()) {
                std::this_thread::yield();
                ec = std::error_code();
                continue;
            }
            return;
        }
        break;
    }
    mUnsignaledCount = 0;
    mOffset += buffer.length();
}

ClientScanQuery::~ClientScanQuery() {
    // Flush buffer
    std::error_code ec;
    mData->done(mBuffer, ec);
    if (ec) {
        LOG_ERROR("Error while flushing buffer [error = %1% %2%]", ec, ec.message());
        return;
    }

    mBuffer.reset();
}

void ClientScanQuery::process(uint64_t validFrom, uint64_t validTo, const char* data, size_t size,
        const Record& /* record */) {
    // Discard the tuple if it is not in the read set or already expired in the readset
    if (!mData->checkTuple(validFrom, validTo)) {
        return;
    }

    mBuffer.add(mData->dataRegion(), data, size);
    if (mBuffer.count() < gMaxBufferTuple) {
        return;
    }

    // TODO Error handling
    std::error_code ec;
    mData->write(mBuffer, ec);
    if (ec) {
        LOG_ERROR("Error while writing buffer [error = %1% %2%]", ec, ec.message());
        return;
    }
    mBuffer.reset();
}

} // namespace store
} // namespace tell
