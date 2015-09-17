#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientManager.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/Record.hpp>
#include <tellstore/ScanMemory.hpp>
#include <tellstore/TransactionRunner.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/string.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <system_error>

using namespace tell;
using namespace tell::store;

namespace {

int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
crossbow::string gTupleText1 = crossbow::string("Bacon ipsum dolor amet t-bone chicken prosciutto, cupim ribeye turkey "
        "bresaola leberkas bacon. Hamburger biltong bresaola, drumstick t-bone flank ball tip.");
crossbow::string gTupleText2 = crossbow::string("Chuck pork loin ham hock tri-tip pork ball tip drumstick tongue. Jowl "
        "swine short loin, leberkas andouille pancetta strip steak doner ham bresaola. T-bone pastrami rump beef ribs, "
        "bacon frankfurter meatball biltong bresaola short ribs.");

class OperationTimer {
public:
    OperationTimer()
            : mTotalDuration(0x0u) {
    }

    void start() {
        mStartTime = std::chrono::steady_clock::now();
    }

    std::chrono::nanoseconds stop() {
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()
                - mStartTime);
        mTotalDuration += duration;
        return duration;
    }

    std::chrono::nanoseconds total() const {
        return mTotalDuration;
    }

private:
    std::chrono::steady_clock::time_point mStartTime;
    std::chrono::nanoseconds mTotalDuration;
};

class TestClient {
public:
    TestClient(const ClientConfig& config, size_t scanMemoryLength, size_t numTuple, size_t numTransactions);

    void run();

    void shutdown();

private:
    void addTable(ClientHandle& client);

    void executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey);

    void executeScan(ClientHandle& handle, float selectivity);

    void executeProjection(ClientHandle& client, float selectivity);

    void executeAggregation(ClientHandle& client, float selectivity);

    ClientManager<void> mManager;

    std::unique_ptr<ScanMemoryManager> mScanMemory;

    /// Number of tuples to insert per transaction
    size_t mNumTuple;

    /// Number of concurrent transactions to start
    size_t mNumTransactions;

    std::array<GenericTuple, 4> mTuple;

    Table mTable;
};

TestClient::TestClient(const ClientConfig& config, size_t scanMemoryLength, size_t numTuple, size_t numTransactions)
        : mManager(config),
          mScanMemory(mManager.allocateScanMemory(config.tellStore.size(), scanMemoryLength / config.tellStore.size())),
          mNumTuple(numTuple),
          mNumTransactions(numTransactions) {
    LOG_INFO("Initialized TellStore client");
    for (decltype(mTuple.size()) i = 0; i < mTuple.size(); ++i) {
        mTuple[i] = GenericTuple({
                std::make_pair<crossbow::string, boost::any>("number", static_cast<int32_t>(i)),
                std::make_pair<crossbow::string, boost::any>("text1", gTupleText1),
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("text2", gTupleText2)
        });
    }
}

void TestClient::run() {
    auto startTime = std::chrono::steady_clock::now();
    LOG_INFO("Starting test workload");
    TransactionRunner<void> runner(mManager);

    LOG_INFO("Start create table transaction");
    runner.executeBlocking(std::bind(&TestClient::addTable, this, std::placeholders::_1));

    LOG_INFO("Starting %1% test load transaction(s)", mNumTransactions);
    for (decltype(mNumTransactions) i = 0; i < mNumTransactions; ++i) {
        auto startRange = i * mNumTuple;
        auto endRange = startRange + mNumTuple;
        runner.execute(std::bind(&TestClient::executeTransaction, this, std::placeholders::_1, startRange, endRange));
    }
    runner.wait();

    LOG_INFO("Starting test scan transaction(s)");
    runner.executeBlocking(std::bind(&TestClient::executeScan, this, std::placeholders::_1, 1.0));
    runner.executeBlocking(std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0.5));
    runner.executeBlocking(std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0.25));
    runner.executeBlocking(std::bind(&TestClient::executeProjection, this, std::placeholders::_1, 1.0));
    runner.executeBlocking(std::bind(&TestClient::executeProjection, this, std::placeholders::_1, 0.5));
    runner.executeBlocking(std::bind(&TestClient::executeProjection, this, std::placeholders::_1, 0.25));
    runner.executeBlocking(std::bind(&TestClient::executeAggregation, this, std::placeholders::_1, 1.0));
    runner.executeBlocking(std::bind(&TestClient::executeAggregation, this, std::placeholders::_1, 0.5));
    runner.executeBlocking(std::bind(&TestClient::executeAggregation, this, std::placeholders::_1, 0.25));

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);
    LOG_INFO("Running test workload took %1%s", duration.count());
}

void TestClient::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    mManager.shutdown();
}

void TestClient::addTable(ClientHandle& client) {
    LOG_TRACE("Adding table");
    Schema schema(TableType::TRANSACTIONAL);
    schema.addField(FieldType::INT, "number", true);
    schema.addField(FieldType::TEXT, "text1", true);
    schema.addField(FieldType::BIGINT, "largenumber", true);
    schema.addField(FieldType::TEXT, "text2", true);

    auto startTime = std::chrono::steady_clock::now();
    mTable = client.createTable("testTable", std::move(schema));
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Adding table took %1%ns", duration.count());
}

void TestClient::executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey) {
    LOG_TRACE("Starting transaction");
    auto transaction = client.startTransaction();
    LOG_INFO("TID %1%] Started transaction", transaction.version());

    OperationTimer insertTimer;
    OperationTimer getTimer;
    auto startTime = std::chrono::steady_clock::now();
    for (auto key = startKey; key < endKey; ++key) {
        LOG_TRACE("Insert tuple");
        insertTimer.start();
        auto insertFuture = transaction.insert(mTable, key, mTuple[key % mTuple.size()], true);
        if (!insertFuture->waitForResult()) {
            auto& ec = insertFuture->error();
            LOG_ERROR("Error inserting tuple [error = %1% %2%]", ec, ec.message());
            return;
        }
        auto insertDuration = insertTimer.stop();
        LOG_DEBUG("Inserting tuple took %1%ns", insertDuration.count());

        auto succeeded = insertFuture->get();
        if (!succeeded) {
            LOG_ERROR("Insert did not succeed");
            return;
        }


        LOG_TRACE("Get tuple");
        getTimer.start();
        auto getFuture = transaction.get(mTable, key);
        if (!getFuture->waitForResult()) {
            auto& ec = getFuture->error();
            LOG_ERROR("Error getting tuple [error = %1% %2%]", ec, ec.message());
            return;
        }
        auto getDuration = getTimer.stop();
        LOG_DEBUG("Getting tuple took %1%ns", getDuration.count());

        auto tuple = getFuture->get();
        if (!tuple->found()) {
            LOG_ERROR("Tuple not found");
            return;
        }
        if (tuple->version() != transaction.version()) {
            LOG_ERROR("Tuple not in the version written");
            return;
        }
        if (!tuple->isNewest()) {
            LOG_ERROR("Tuple not the newest");
            return;
        }

        LOG_TRACE("Check tuple");
        if (mTable.field<int32_t>("number", tuple->data()) != static_cast<int32_t>(key % mTuple.size())) {
            LOG_ERROR("Number value does not match");
            return;
        }
        if (mTable.field<crossbow::string>("text1", tuple->data()) != gTupleText1) {
            LOG_ERROR("Text1 value does not match");
            return;
        }
        if (mTable.field<int64_t>("largenumber", tuple->data()) != gTupleLargenumber) {
            LOG_ERROR("Text2 value does not match");
            return;
        }
        if (mTable.field<crossbow::string>("text2", tuple->data()) != gTupleText2) {
            LOG_ERROR("Text2 value does not match");
            return;
        }
        LOG_TRACE("Tuple check successful");
    }

    LOG_TRACE("Commit transaction");
    transaction.commit();

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    LOG_INFO("TID %1%] Transaction completed in %2%ms [total = %3%ms / %4%ms, average = %5%us / %6%us]",
             transaction.version(),
             duration.count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(insertTimer.total()).count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(getTimer.total()).count(),
             std::chrono::duration_cast<std::chrono::microseconds>(insertTimer.total()).count() / (endKey - startKey),
             std::chrono::duration_cast<std::chrono::microseconds>(getTimer.total()).count() / (endKey - startKey));
}

void TestClient::executeScan(ClientHandle& client, float selectivity) {
    LOG_TRACE("Starting transaction");
    auto& fiber = client.fiber();
    auto transaction = client.startTransaction(true);
    LOG_INFO("TID %1%] Starting full scan with selectivity %2%%%", transaction.version(),
            static_cast<int>(selectivity * 100));

    Record::id_t recordField;
    if (!mTable.record().idOf("number", recordField)) {
        LOG_ERROR("number field not found");
        return;
    }

    uint32_t selectionLength = 24;
    std::unique_ptr<char[]> selection(new char[selectionLength]);

    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
    selectionWriter.write<uint64_t>(0x1u);
    selectionWriter.write<uint16_t>(recordField);
    selectionWriter.write<uint16_t>(0x1u);
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    selectionWriter.write<uint8_t>(0x0u);
    selectionWriter.align(sizeof(uint32_t));
    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanFutures = transaction.scan(mTable, *mScanMemory, ScanQueryType::FULL, selectionLength, selection.get(),
            0x0u, nullptr);
    for (auto& future : scanFutures) {
        if (!future->get()) {
            LOG_ERROR("Error scanning table");
            return;
        }
        fiber.yield();

        LOG_ASSERT(future.unique(), "Future pointer should be unique at this point");
    }
    auto scanEndTime = std::chrono::steady_clock::now();

    size_t scanCount = 0x0u;
    size_t scanDataSize = 0x0u;
    for (auto& future : scanFutures) {
        while (future->hasNext()) {
            uint64_t key;
            const char* tuple;
            size_t tupleLength;
            std::tie(key, tuple, tupleLength) = future->next();
            ++scanCount;
            scanDataSize += tupleLength;

            LOG_TRACE("Check tuple");
            if (mTable.field<int32_t>("number", tuple) != static_cast<int32_t>(key % mTuple.size())) {
                LOG_ERROR("Number value of tuple %1% does not match", scanCount);
                return;
            }
            if (mTable.field<crossbow::string>("text1", tuple) != gTupleText1) {
                LOG_ERROR("Text1 value does not match of tuple %1%", scanCount);
                return;
            }
            if (mTable.field<int64_t>("largenumber", tuple) != gTupleLargenumber) {
                LOG_ERROR("largenumber value of tuple %1% does not match", scanCount);
                return;
            }
            if (mTable.field<crossbow::string>("text2", tuple) != gTupleText2) {
                LOG_ERROR("Text2 value of tuple %1% does not match", scanCount);
                return;
            }
            LOG_TRACE("Tuple check successful");
            if (scanCount % 250 == 0) {
                fiber.yield();
            }
        }
    }

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    auto scanTupleSize = scanDataSize / scanCount;
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
            transaction.version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
}

void TestClient::executeProjection(ClientHandle& client, float selectivity) {
    LOG_TRACE("Starting transaction");
    auto& fiber = client.fiber();
    auto transaction = client.startTransaction(true);
    LOG_INFO("TID %1%] Starting projection scan with selectivity %2%%%", transaction.version(),
            static_cast<int>(selectivity * 100));

    Record::id_t numberField;
    if (!mTable.record().idOf("number", numberField)) {
        LOG_ERROR("number field not found");
        return;
    }

    Record::id_t text2Field;
    if (!mTable.record().idOf("text2", text2Field)) {
        LOG_ERROR("text2 field not found");
        return;
    }

    uint32_t selectionLength = 24;
    std::unique_ptr<char[]> selection(new char[selectionLength]);

    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
    selectionWriter.write<uint64_t>(0x1u);
    selectionWriter.write<uint16_t>(numberField);
    selectionWriter.write<uint16_t>(0x1u);
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    selectionWriter.write<uint8_t>(0x0u);
    selectionWriter.align(sizeof(uint32_t));
    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    uint32_t projectionLength = 4;
    std::unique_ptr<char[]> projection(new char[projectionLength]);

    crossbow::buffer_writer projectionWriter(projection.get(), projectionLength);
    projectionWriter.write<uint16_t>(numberField);
    projectionWriter.write<uint16_t>(text2Field);

    Schema resultSchema(mTable.tableType());
    resultSchema.addField(FieldType::INT, "number", true);
    resultSchema.addField(FieldType::TEXT, "text2", true);
    Table resultTable(mTable.tableId(), std::move(resultSchema));

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanFutures = transaction.scan(resultTable, *mScanMemory, ScanQueryType::PROJECTION, selectionLength,
            selection.get(), projectionLength, projection.get());
    for (auto& future : scanFutures) {
        if (!future->get()) {
            LOG_ERROR("Error scanning table");
            return;
        }
        fiber.yield();

        LOG_ASSERT(future.unique(), "Future pointer should be unique at this point");
    }
    auto scanEndTime = std::chrono::steady_clock::now();

    size_t scanCount = 0x0u;
    size_t scanDataSize = 0x0u;
    for (auto& future : scanFutures) {
        while (future->hasNext()) {
            uint64_t key;
            const char* tuple;
            size_t tupleLength;
            std::tie(key, tuple, tupleLength) = future->next();
            ++scanCount;
            scanDataSize += tupleLength;

            LOG_TRACE("Check tuple");
            if (resultTable.field<int32_t>("number", tuple) != static_cast<int32_t>(key % mTuple.size())) {
                LOG_ERROR("Number value of tuple %1% does not match", scanCount);
                return;
            }
            if (resultTable.field<crossbow::string>("text2", tuple) != gTupleText2) {
                LOG_ERROR("Text2 value of tuple %1% does not match", scanCount);
                return;
            }
            LOG_TRACE("Tuple check successful");
            if (scanCount % 250 == 0) {
                fiber.yield();
            }
        }
    }

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    auto scanTupleSize = scanDataSize / scanCount;
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
            transaction.version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
}

void TestClient::executeAggregation(ClientHandle& client, float selectivity) {
    LOG_TRACE("Starting transaction");
    auto& fiber = client.fiber();
    auto transaction = client.startTransaction(true);
    LOG_INFO("TID %1%] Starting aggregation scan with selectivity %2%%%", transaction.version(),
            static_cast<int>(selectivity * 100));

    Record::id_t recordField;
    if (!mTable.record().idOf("number", recordField)) {
        LOG_ERROR("number field not found");
        return;
    }

    uint32_t selectionLength = 24;
    std::unique_ptr<char[]> selection(new char[selectionLength]);

    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
    selectionWriter.write<uint64_t>(0x1u);
    selectionWriter.write<uint16_t>(recordField);
    selectionWriter.write<uint16_t>(0x1u);
    selectionWriter.align(sizeof(uint64_t));
    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    selectionWriter.write<uint8_t>(0x0u);
    selectionWriter.align(sizeof(uint32_t));
    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    uint32_t aggregationLength = 12;
    std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

    crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::SUM));
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::MIN));
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::MAX));

    Schema resultSchema(mTable.tableType());
    resultSchema.addField(FieldType::INT, "sum", true);
    resultSchema.addField(FieldType::INT, "min", true);
    resultSchema.addField(FieldType::INT, "max", true);
    Table resultTable(mTable.tableId(), std::move(resultSchema));

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanFutures = transaction.scan(resultTable, *mScanMemory, ScanQueryType::AGGREGATION, selectionLength,
            selection.get(), aggregationLength, aggregation.get());
    for (auto& future : scanFutures) {
        if (!future->get()) {
            LOG_ERROR("Error scanning table");
            return;
        }
        fiber.yield();

        LOG_ASSERT(future.unique(), "Future pointer should be unique at this point");
    }
    auto scanEndTime = std::chrono::steady_clock::now();

    size_t scanCount = 0x0u;
    size_t scanDataSize = 0x0u;
    int32_t totalSum = 0;
    int32_t totalMin = std::numeric_limits<int32_t>::max();
    int32_t totalMax = std::numeric_limits<int32_t>::min();
    for (auto& future : scanFutures) {
        while (future->hasNext()) {
            const char* tuple;
            size_t tupleLength;
            std::tie(std::ignore, tuple, tupleLength) = future->next();
            ++scanCount;
            scanDataSize += tupleLength;

            totalSum += resultTable.field<int32_t>("sum", tuple);
            totalMin = std::min(totalMin, resultTable.field<int32_t>("min", tuple));
            totalMax = std::max(totalMax, resultTable.field<int32_t>("max", tuple));

            if (scanCount % 250 == 0) {
                fiber.yield();
            }
        }
    }

    LOG_INFO("TID %1%] Scan output [sum = %2%, min = %3%, max = %4%]", transaction.version(), totalSum, totalMin,
            totalMax);

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    auto scanTupleSize = scanDataSize / scanCount;
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
            transaction.version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
}

} // anonymous namespace

int main(int argc, const char** argv) {
    crossbow::string commitManagerHost;
    crossbow::string tellStoreHost;
    size_t scanMemoryLength = 0x80000000ull;
    size_t numTuple = 1000000ull;
    size_t numTransactions = 10;
    tell::store::ClientConfig clientConfig;
    bool help = false;
    crossbow::string logLevel("DEBUG");

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'c'>("commit-manager", &commitManagerHost),
            crossbow::program_options::value<'s'>("server", &tellStoreHost),
            crossbow::program_options::value<'m'>("memory", &scanMemoryLength),
            crossbow::program_options::value<'n'>("tuple", &numTuple),
            crossbow::program_options::value<'t'>("transactions", &numTransactions),
            crossbow::program_options::value<-1>("network-threads", &clientConfig.numNetworkThreads,
                    crossbow::program_options::tag::ignore_short<true>{}));

    try {
        crossbow::program_options::parse(opts, argc, argv);
    } catch (crossbow::program_options::argument_not_found e) {
        std::cerr << e.what() << std::endl << std::endl;
        crossbow::program_options::print_help(std::cout, opts);
        return 1;
    }

    if (help) {
        crossbow::program_options::print_help(std::cout, opts);
        return 0;
    }

    clientConfig.commitManager = ClientConfig::parseCommitManager(commitManagerHost);
    clientConfig.tellStore = ClientConfig::parseTellStore(tellStoreHost);

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    LOG_INFO("Starting TellStore client");
    LOG_INFO("--- Commit Manager: %1%", clientConfig.commitManager);
    for (auto& ep : clientConfig.tellStore) {
        LOG_INFO("--- TellStore Shards: %1%", ep);
    }
    LOG_INFO("--- Network Threads: %1%", clientConfig.numNetworkThreads);
    LOG_INFO("--- Scan Memory: %1%GB", double(scanMemoryLength) / double(1024 * 1024 * 1024));
    LOG_INFO("--- Number of tuples: %1%", numTuple);
    LOG_INFO("--- Number of transactions: %1%", numTransactions);

    // Initialize network stack
    TestClient client(clientConfig, scanMemoryLength, numTuple, numTransactions);
    client.run();
    client.shutdown();

    LOG_INFO("Exiting TellStore client");
    return 0;
}
