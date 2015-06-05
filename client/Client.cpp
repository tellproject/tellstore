#include "Client.hpp"

#include <util/CommitManager.hpp>
#include <util/Record.hpp>
#include <util/Logging.hpp>

#include <chrono>
#include <functional>
#include <system_error>

namespace tell {
namespace store {

namespace {

int32_t gTupleNumber = 12;
int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
crossbow::string gTupleText1 = crossbow::string("Bacon ipsum dolor amet t-bone chicken prosciutto, cupim ribeye turkey "
        "bresaola leberkas bacon. Hamburger biltong bresaola, drumstick t-bone flank ball tip.");
crossbow::string gTupleText2 = crossbow::string("Chuck pork loin ham hock tri-tip pork ball tip drumstick tongue. Jowl "
        "swine short loin, leberkas andouille pancetta strip steak doner ham bresaola. T-bone pastrami rump beef ribs, "
        "bacon frankfurter meatball biltong bresaola short ribs.");

} // anonymous namespace

void Client::init() {
    LOG_INFO("Initializing TellStore client");

    std::error_code ec;
    mManager.init(mConfig, ec);
    if (ec) {
        LOG_ERROR("Failure init [error = %1% %2%]", ec, ec.message());
    }
    LOG_DEBUG("Start transaction");
    mManager.executeTransaction(std::bind(&Client::addTable, this, std::placeholders::_1));
    mService.run();
}

void Client::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    // TODO
}

void Client::addTable(Transaction& transaction) {
    std::error_code ec;

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
    LOG_INFO("TID %1%] Adding table took %2%ns", transaction.id(), duration.count());

    size_t numTuple = 1000000ull;
    size_t numTransactions = 10;

    for (size_t i = 0; i < numTransactions; ++i) {
        ++mActiveTransactions;
        mManager.executeTransaction(std::bind(&Client::executeTransaction, this, std::placeholders::_1, i * numTuple,
                (i + 1) * numTuple));
    }
}

void Client::executeTransaction(Transaction& transaction, uint64_t startKey, uint64_t endKey) {
    LOG_DEBUG("TID %1%] Starting transaction", transaction.id());

    std::error_code ec;

    Record record(mSchema);
    auto snapshot = mCommitManager.startTx();
    bool succeeded = false;

    GenericTuple insertTuple({
            std::make_pair<crossbow::string, boost::any>("number", gTupleNumber),
            std::make_pair<crossbow::string, boost::any>("text1", gTupleText1),
            std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
            std::make_pair<crossbow::string, boost::any>("text2", gTupleText2)
    });
    size_t insertSize = 0;
    auto insertData = record.create(insertTuple, insertSize);

    std::chrono::nanoseconds totalInsertDuration(0x0u);
    std::chrono::nanoseconds totalGetDuration(0x0u);
    auto startTime = std::chrono::steady_clock::now();
    for (auto key = startKey; key < endKey; ++key) {
        LOG_TRACE("Insert tuple");
        auto insertStartTime = std::chrono::steady_clock::now();
        transaction.insert(mTableId, key, insertSize, insertData, snapshot, ec, &succeeded);
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
        auto numberData = getTupleData(getData, record, "number");
        if (*reinterpret_cast<const int32_t*>(numberData) != gTupleNumber) {
            LOG_ERROR("Number value does not match");
        }
        auto text1Data = getTupleData(getData, record, "text1");
        uint32_t text1Length = *reinterpret_cast<const uint32_t*>(text1Data);
        if (crossbow::string(text1Data + 4, text1Length) != gTupleText1) {
            LOG_ERROR("Text1 value does not match");
        }
        auto largenumberData = getTupleData(getData, record, "largenumber");
        if (*reinterpret_cast<const int64_t*>(largenumberData) != gTupleLargenumber) {
            LOG_ERROR("Large Number value not something large");
        }
        auto text2Data = getTupleData(getData, record, "text2");
        uint32_t text2Length = *reinterpret_cast<const uint32_t*>(text2Data);
        if (crossbow::string(text2Data + 4, text2Length) != gTupleText2) {
            LOG_ERROR("Text2 value does not match");
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

    mCommitManager.commitTx(snapshot);
    if (--mActiveTransactions != 0) {
        return;
    }

    auto scanSnapshot = mCommitManager.startTx();
    uint64_t numColumns = 0x0u;
    char* query = reinterpret_cast<char*>(&numColumns);
    uint32_t querySize = 8;

    LOG_INFO("TID %1%] Starting scan", transaction.id());

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanCount = transaction.scan(mTableId, querySize, query, scanSnapshot, ec);
    auto scanEndTime = std::chrono::steady_clock::now();
    if (ec) {
        LOG_ERROR("Error scanning [error = %1% %2%]", ec, ec.message());
        return;
    }

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanDataSize = double(scanCount * insertSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanCount * insertSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of size %4% (%5%GB total, %6%Gbps bandwidth)]", transaction.id(),
            scanDuration.count(), scanCount, insertSize, scanDataSize, scanBandwidth);
}

const char* Client::getTupleData(const char* data, Record& record, const crossbow::string& name) {
    Record::id_t recordField;
    if (!record.idOf(name, recordField)) {
        LOG_ERROR("%1% field not found", name);
    }
    bool fieldIsNull;
    auto fieldData = record.data(data, recordField, fieldIsNull);
    return fieldData;
}

} // namespace store
} // namespace tell
