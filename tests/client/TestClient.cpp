#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientManager.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/Record.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/ByteBuffer.hpp>
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
    TestClient(crossbow::infinio::InfinibandService& service, const ClientConfig& config, size_t numTuple,
            size_t numTransactions);

    void shutdown();

private:
    void addTable(ClientHandle& client);

    void executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey);

    void doScan(ClientTransaction& transaction, const Table& record, float selectivity);

    ClientManager mManager;

    /// Number of tuples to insert per transaction
    size_t mNumTuple;

    /// Number of concurrent transactions to start
    size_t mNumTransactions;

    std::atomic<size_t> mActiveTransactions;

    uint64_t mTupleSize;
    std::array<GenericTuple, 4> mTuple;
};

TestClient::TestClient(crossbow::infinio::InfinibandService& service, const ClientConfig& config, size_t numTuple,
        size_t numTransactions)
        : mManager(service, config),
          mNumTuple(numTuple),
          mNumTransactions(numTransactions),
          mActiveTransactions(0),
          mTupleSize(0x0u) {
    LOG_INFO("Initialized TellStore client");
    for (int32_t i = 0; i < mTuple.size(); ++i) {
        mTuple[i] = GenericTuple({
                std::make_pair<crossbow::string, boost::any>("number", i),
                std::make_pair<crossbow::string, boost::any>("text1", gTupleText1),
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("text2", gTupleText2)
        });
    }

    LOG_DEBUG("Start transaction");
    mManager.execute(std::bind(&TestClient::addTable, this, std::placeholders::_1));
}

void TestClient::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    // TODO
}

void TestClient::addTable(ClientHandle& client) {
    LOG_TRACE("Adding table");
    Schema schema(TableType::TRANSACTIONAL);
    schema.addField(FieldType::INT, "number", true);
    schema.addField(FieldType::TEXT, "text1", true);
    schema.addField(FieldType::BIGINT, "largenumber", true);
    schema.addField(FieldType::TEXT, "text2", true);

    auto startTime = std::chrono::steady_clock::now();
    auto createTableFuture = client.createTable("testTable", schema);
    if (!createTableFuture->waitForResult()) {
        auto& ec = createTableFuture->error();
        LOG_ERROR("Error adding table [error = %1% %2%]", ec, ec.message());
        return;
    }
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Adding table took %1%ns", duration.count());

    auto table = createTableFuture->get();
    mTupleSize = table->record().sizeOfTuple(mTuple[0]);

    for (size_t i = 0; i < mNumTransactions; ++i) {
        auto startRange = i * mNumTuple;
        auto endRange = startRange + mNumTuple;
        ++mActiveTransactions;
        mManager.execute(std::bind(&TestClient::executeTransaction, this, std::placeholders::_1, startRange, endRange));
    }
}

void TestClient::executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey) {
    LOG_TRACE("Opening table");
    auto openTableStartTime = std::chrono::steady_clock::now();
    auto openTableFuture = client.getTable("testTable");
    if (!openTableFuture->waitForResult()) {
        auto& ec = openTableFuture->error();
        LOG_ERROR("Error opening table [error = %1% %2%]", ec, ec.message());
    }
    auto openTableEndTime = std::chrono::steady_clock::now();
    auto openTableDuration = std::chrono::duration_cast<std::chrono::nanoseconds>(openTableEndTime
            - openTableStartTime);
    auto table = openTableFuture->get();
    LOG_TRACE("Opening table took %1%ns", openTableDuration.count());

    LOG_TRACE("Starting transaction");
    auto transaction = client.startTransaction();
    LOG_INFO("TID %1%] Started transaction", transaction.version());


    OperationTimer insertTimer;
    OperationTimer getTimer;
    auto startTime = std::chrono::steady_clock::now();
    for (auto key = startKey; key < endKey; ++key) {
        LOG_TRACE("Insert tuple");
        insertTimer.start();
        auto insertFuture = transaction.insert(*table, key, mTuple[key % mTuple.size()], true);
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
        auto getFuture = transaction.get(*table, key);
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
        if (table->field<int32_t>("number", tuple->data()) != (key % mTuple.size())) {
            LOG_ERROR("Number value does not match");
            return;
        }
        if (table->field<crossbow::string>("text1", tuple->data()) != gTupleText1) {
            LOG_ERROR("Text1 value does not match");
            return;
        }
        if (table->field<int64_t>("largenumber", tuple->data()) != gTupleLargenumber) {
            LOG_ERROR("Text2 value does not match");
            return;
        }
        if (table->field<crossbow::string>("text2", tuple->data()) != gTupleText2) {
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

    if (--mActiveTransactions != 0) {
        return;
    }

    auto scanTransaction = client.startTransaction();
    doScan(scanTransaction, *table, 1.0);
    doScan(scanTransaction, *table, 0.5);
    doScan(scanTransaction, *table, 0.25);
}

void TestClient::doScan(ClientTransaction& transaction, const Table& table, float selectivity) {
    LOG_INFO("TID %1%] Starting scan with selectivity %2%%%", transaction.version(),
            static_cast<int>(selectivity * 100));

    Record::id_t recordField;
    if (!table.record().idOf("number", recordField)) {
        LOG_ERROR("number field not found");
        return;
    }

    uint32_t queryLength = 24;
    std::unique_ptr<char[]> query(new char[queryLength]);

    crossbow::infinio::BufferWriter queryWriter(query.get(), queryLength);
    queryWriter.write<uint64_t>(0x1u);
    queryWriter.write<uint16_t>(recordField);
    queryWriter.write<uint16_t>(0x1u);
    queryWriter.align(sizeof(uint64_t));
    queryWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
    queryWriter.write<uint8_t>(0x0u);
    queryWriter.align(sizeof(uint32_t));
    queryWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);

    size_t scanCount = 0x0u;
    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanFuture = transaction.scan(table, queryLength, query.get());
    while (scanFuture->hasNext()) {
        auto tuple = scanFuture->next();
        ++scanCount;
    }
    auto scanEndTime = std::chrono::steady_clock::now();
    if (!scanFuture->waitForResult()) {
        auto& ec = scanFuture->error();
        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
        return;
    }

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanDataSize = double(scanCount * mTupleSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanCount * mTupleSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of size %4% (%5%GB total, %6%Gbps bandwidth)]",
            transaction.version(), scanDuration.count(), scanCount, mTupleSize, scanDataSize, scanBandwidth);
}

} // anonymous namespace

int main(int argc, const char** argv) {
    crossbow::string commitManagerHost;
    crossbow::string tellStoreHost;
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
            crossbow::program_options::value<'m'>("memory", &clientConfig.scanMemory),
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

    clientConfig.commitManager = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManagerHost);
    clientConfig.tellStore = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), tellStoreHost);

    crossbow::infinio::InfinibandLimits infinibandLimits;
    infinibandLimits.receiveBufferCount = 128;
    infinibandLimits.sendBufferCount = 128;
    infinibandLimits.bufferLength = 32 * 1024;
    infinibandLimits.sendQueueLength = 128;

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    LOG_INFO("Starting TellStore client");
    LOG_INFO("--- Commit Manager: %1%", clientConfig.commitManager);
    LOG_INFO("--- TellStore: %1%", clientConfig.tellStore);
    LOG_INFO("--- Network Threads: %1%", clientConfig.numNetworkThreads);
    LOG_INFO("--- Scan Memory: %1%GB", double(clientConfig.scanMemory) / double(1024 * 1024 * 1024));
    LOG_INFO("--- Number of tuples: %1%", numTuple);
    LOG_INFO("--- Number of transactions: %1%", numTransactions);

    // Initialize network stack
    crossbow::infinio::InfinibandService service(infinibandLimits);
    TestClient client(service, clientConfig, numTuple, numTransactions);
    service.run();

    LOG_INFO("Exiting TellStore client");
    return 0;
}
