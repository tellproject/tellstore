#include "Client.hpp"

#include <util/CommitManager.hpp>
#include <util/Record.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/ByteBuffer.hpp>
#include <crossbow/logger.hpp>

#include <chrono>
#include <functional>
#include <system_error>

namespace tell {
namespace store {

namespace {

int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
crossbow::string gTupleText1 = crossbow::string("Bacon ipsum dolor amet t-bone chicken prosciutto, cupim ribeye turkey "
        "bresaola leberkas bacon. Hamburger biltong bresaola, drumstick t-bone flank ball tip.");
crossbow::string gTupleText2 = crossbow::string("Chuck pork loin ham hock tri-tip pork ball tip drumstick tongue. Jowl "
        "swine short loin, leberkas andouille pancetta strip steak doner ham bresaola. T-bone pastrami rump beef ribs, "
        "bacon frankfurter meatball biltong bresaola short ribs.");

} // anonymous namespace

Client::Client(const ClientConfig& config)
        : mConfig(config),
          mService(mConfig.infinibandLimits),
          mManager(mService, mConfig),
          mTableId(0x0u),
          mActiveTransactions(0),
          mTupleSize(0) {
    LOG_INFO("Initialized TellStore client");

    mSchema.addField(FieldType::INT, "number", true);
    mSchema.addField(FieldType::TEXT, "text1", true);
    mSchema.addField(FieldType::BIGINT, "largenumber", true);
    mSchema.addField(FieldType::TEXT, "text2", true);

    Record record(mSchema);
    for (int32_t i = 0; i < mTuple.size(); ++i) {
        GenericTuple insertTuple({
                std::make_pair<crossbow::string, boost::any>("number", i),
                std::make_pair<crossbow::string, boost::any>("text1", gTupleText1),
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("text2", gTupleText2)
        });
        mTuple[i].reset(record.create(insertTuple, mTupleSize));
    }
}

void Client::init() {
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

    for (size_t i = 0; i < mConfig.numTransactions; ++i) {
        auto startRange = i * mConfig.numTuple;
        auto endRange = startRange + mConfig.numTuple;
        ++mActiveTransactions;
        mManager.executeTransaction(std::bind(&Client::executeTransaction, this, std::placeholders::_1, startRange,
                endRange));
    }
}

void Client::executeTransaction(Transaction& transaction, uint64_t startKey, uint64_t endKey) {
    LOG_DEBUG("TID %1%] Starting transaction", transaction.id());

    std::error_code ec;

    Record record(mSchema);
    auto snapshot = mCommitManager.startTx();
    bool succeeded = false;

    std::chrono::nanoseconds totalInsertDuration(0x0u);
    std::chrono::nanoseconds totalGetDuration(0x0u);
    auto startTime = std::chrono::steady_clock::now();
    for (auto key = startKey; key < endKey; ++key) {
        LOG_TRACE("Insert tuple");
        auto insertStartTime = std::chrono::steady_clock::now();
        transaction.insert(mTableId, key, mTupleSize, mTuple[key % mTuple.size()].get(), snapshot, ec, &succeeded);
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
        uint64_t version = 0x0u;
        bool isNewest = false;
        auto getStartTime = std::chrono::steady_clock::now();
        succeeded = transaction.get(mTableId, key, getSize, getData, snapshot, version, isNewest, ec);
        auto getEndTime = std::chrono::steady_clock::now();
        if (ec) {
            LOG_ERROR("Error getting tuple [error = %1% %2%]", ec, ec.message());
            return;
        }
        if (!succeeded) {
            LOG_ERROR("Tuple not found");
            return;
        }
        if (version != snapshot.version()) {
            LOG_ERROR("Tuple not in the version written");
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
        if (*reinterpret_cast<const int32_t*>(numberData) != (key % mTuple.size())) {
            LOG_ERROR("Number value does not match");
            return;
        }
        auto text1Data = getTupleData(getData, record, "text1");
        uint32_t text1Length = *reinterpret_cast<const uint32_t*>(text1Data);
        if (crossbow::string(text1Data + 4, text1Length) != gTupleText1) {
            LOG_ERROR("Text1 value does not match");
            return;
        }
        auto largenumberData = getTupleData(getData, record, "largenumber");
        if (*reinterpret_cast<const int64_t*>(largenumberData) != gTupleLargenumber) {
            LOG_ERROR("Large Number value not something large");
            return;
        }
        auto text2Data = getTupleData(getData, record, "text2");
        uint32_t text2Length = *reinterpret_cast<const uint32_t*>(text2Data);
        if (crossbow::string(text2Data + 4, text2Length) != gTupleText2) {
            LOG_ERROR("Text2 value does not match");
            return;
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

    doScan(transaction, record, 1.0, scanSnapshot);
    doScan(transaction, record, 0.5, scanSnapshot);
    doScan(transaction, record, 0.25, scanSnapshot);
}

void Client::doScan(Transaction& transaction, Record& record, float selectivity, const SnapshotDescriptor& snapshot) {
    Record::id_t recordField;
    if (!record.idOf("number", recordField)) {
        LOG_ERROR("number field not found");
        return;
    }

    uint32_t querySize = 24;
    std::unique_ptr<char[]> query(new char[querySize]);

    crossbow::infinio::BufferWriter queryWriter(query.get(), querySize);
    queryWriter.write<uint64_t>(0x1u);
    queryWriter.write<uint16_t>(recordField);
    queryWriter.write<uint16_t>(0x1u);
    queryWriter.align(sizeof(uint64_t));
    queryWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    queryWriter.write<uint8_t>(0x0u);
    queryWriter.align(sizeof(uint32_t));
    queryWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    LOG_INFO("TID %1%] Starting scan with selectivity %2%%%", transaction.id(), static_cast<int>(selectivity * 100));

    std::error_code ec;
    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanCount = transaction.scan(mTableId, querySize, query.get(), snapshot, ec);
    auto scanEndTime = std::chrono::steady_clock::now();
    if (ec) {
        LOG_ERROR("Error scanning [error = %1% %2%]", ec, ec.message());
        return;
    }

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanDataSize = double(scanCount * mTupleSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanCount * mTupleSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of size %4% (%5%GB total, %6%Gbps bandwidth)]", transaction.id(),
            scanDuration.count(), scanCount, mTupleSize, scanDataSize, scanBandwidth);
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
