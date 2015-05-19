#include "Client.hpp"

#include <util/CommitManager.hpp>
#include <util/Record.hpp>
#include <util/Logging.hpp>

#include <crossbow/infinio/EventDispatcher.hpp>

#include <boost/system/error_code.hpp>

#include <chrono>
#include <functional>

namespace tell {
namespace store {

void Client::init() {
    LOG_INFO("Initializing TellStore client");

    boost::system::error_code ec;
    mManager.init(mConfig, ec, [this] () {
        LOG_DEBUG("Start transaction");
        auto trans = mManager.startTransaction();
        auto res = trans->execute(std::bind(&Client::addTable, this, std::placeholders::_1));
        if (!res) {
            LOG_ERROR("Unable to execute transaction function");
        }
        tbb::spin_mutex::scoped_lock lock(mTransMutex);
        mTrans.emplace_back(std::move(trans));
    });
    if (ec) {
        LOG_ERROR("Failure init [error = %1% %2%]", ec, ec.message());
    }
}

void Client::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    // TODO
}

void Client::addTable(Transaction& transaction) {
    boost::system::error_code ec;

    LOG_TRACE("Adding table");
    auto startTime = std::chrono::steady_clock::now();
    auto res = transaction.createTable("testTable", mSchema, mTableId, ec);
    auto endTime = std::chrono::steady_clock::now();
    if (ec) {
        LOG_ERROR("Error adding table [error = %1% %2%]", ec, ec.message());
        return;
    }
    if (!res) {
        LOG_ERROR("Table already exists");
        return;
    }
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Adding table took %1%ns", duration.count());

    for (auto i = 0; i < 10; ++i) {
        mDispatcher.post([this, i] () {
            auto trans = mManager.startTransaction();
            auto res = trans->execute(std::bind(&Client::executeTransaction, this, std::placeholders::_1,
                    i * 10000 + 1, (i + 1) * 10000 + 1));
            if (!res) {
                LOG_ERROR("Unable to execute transaction function");
            }
            tbb::spin_mutex::scoped_lock lock(mTransMutex);
            mTrans.emplace_back(std::move(trans));
        });
    }
}

void Client::executeTransaction(Transaction& transaction, uint64_t startKey, uint64_t endKey) {
    LOG_DEBUG("TID %1%] Starting transaction", transaction.id());

    boost::system::error_code ec;

    Record record(mSchema);
    auto snapshot = mCommitManager.startTx();
    bool succeeded = false;

    std::chrono::nanoseconds totalInsertDuration(0x0u);
    std::chrono::nanoseconds totalGetDuration(0x0u);
    auto startTime = std::chrono::steady_clock::now();
    for (auto key = startKey; key < endKey; ++key) {
        LOG_TRACE("Insert tuple");
        GenericTuple insertTuple({std::make_pair<crossbow::string, boost::any>("foo", 12)});
        size_t insertSize;
        std::unique_ptr<char[]> insertData(record.create(insertTuple, insertSize));
        auto insertStartTime = std::chrono::steady_clock::now();
        transaction.insert(mTableId, key, insertSize, insertData.get(), snapshot, ec, &succeeded);
        auto insertEndTime = std::chrono::steady_clock::now();
        if (ec) {
            LOG_ERROR("Error inserting tuple [error = %1% %2%]", ec, ec.message());
            return;
        }
        if (!succeeded) {
            LOG_ERROR("Insert did not succeed");
            return;
        }
        auto insertDuration = std::chrono::duration_cast<std::chrono::nanoseconds>(insertEndTime - insertStartTime);
        totalInsertDuration += insertDuration;
        LOG_DEBUG("Inserting tuple took %1%ns", insertDuration.count());

        LOG_TRACE("Get tuple");
        size_t getSize;
        const char* getData;
        bool isNewest = false;
        auto getStartTime = std::chrono::steady_clock::now();
        succeeded = transaction.get(mTableId, key, getSize, getData, snapshot, isNewest, ec);
        auto getEndTime = std::chrono::steady_clock::now();
        if (ec) {
            LOG_ERROR("Error getting tuple [error = %1% %2%]", ec, ec.message());
            return;
        }
        if (!succeeded) {
            LOG_ERROR("Tuple not found");
            return;
        }
        if (!isNewest) {
            LOG_ERROR("Tuple not the newest");
            return;
        }
        auto getDuration = std::chrono::duration_cast<std::chrono::nanoseconds>(getEndTime - getStartTime);
        totalGetDuration += getDuration;
        LOG_DEBUG("Getting tuple took %1%ns", getDuration.count());

        LOG_TRACE("Check tuple");
        Record::id_t fooField;
        if (!record.idOf("foo", fooField)) {
            LOG_ERROR("foo field not found");
        }
        bool fooIsNull;
        auto fooData = record.data(getData, fooField, fooIsNull);
        if (*reinterpret_cast<const int32_t*>(fooData) != 12) {
            LOG_ERROR("Tuple value not 12");
        }
        LOG_TRACE("Tuple check successful");
    }
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    LOG_INFO("TID %1%] Transaction completed in %2%ms [total = %3%ms / %4%ms, average = %5%us / %6%us]",
             transaction.id(),
             duration.count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(totalInsertDuration).count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(totalGetDuration).count(),
             std::chrono::duration_cast<std::chrono::microseconds>(totalInsertDuration).count() / (endKey - startKey),
             std::chrono::duration_cast<std::chrono::microseconds>(totalGetDuration).count() / (endKey - startKey));
}

} // namespace store
} // namespace tell
