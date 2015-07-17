#include "ClientManager.hpp"

#include "ClientConfig.hpp"

#include <crossbow/logger.hpp>

#include <sys/mman.h>

namespace tell {
namespace store {
namespace {

void checkTableType(const Table& table, TableType type) {
    if (table.tableType() != type) {
        throw std::logic_error("Operation not supported on table");
    }
}

SnapshotDescriptor buildNonTransactionalSnapshot(uint64_t version) {
    auto descLen = 2 * sizeof(uint64_t) + sizeof(uint8_t);
    auto desc = new char[descLen];
    crossbow::infinio::BufferWriter writer(desc, descLen);
    writer.write<uint64_t>(version); // Base Version
    writer.write<uint64_t>(0x0u); // Lowest Active Version
    writer.write<uint8_t>(0x0u); // Versions

    return {reinterpret_cast<unsigned char*>(desc), descLen, version + 1};
}

} // anonymous namespace

ClientTransaction::ClientTransaction(ClientProcessor& processor, crossbow::infinio::Fiber& fiber,
        SnapshotDescriptor snapshot)
        : mProcessor(processor),
          mFiber(fiber),
          mSnapshot(std::move(snapshot)),
          mCommitted(false) {
    mModified.set_empty_key(std::make_tuple(0x0u, 0x0u));
}

ClientTransaction::~ClientTransaction() {
    if (!mCommitted) {
        abort();
    }
}

ClientTransaction::ClientTransaction(ClientTransaction&& other)
        : mProcessor(other.mProcessor),
          mFiber(other.mFiber),
          mSnapshot(std::move(other.mSnapshot)),
          mModified(std::move(other.mModified)),
          mCommitted(other.mCommitted) {
    other.mCommitted = true;
}

std::shared_ptr<GetResponse> ClientTransaction::get(const Table& table, uint64_t key) {
    return executeInTransaction<GetResponse>(table, [this, &table, key] () {
        return mProcessor.get(mFiber, table.tableId(), key, mSnapshot);
    });
}

std::shared_ptr<ModificationResponse> ClientTransaction::insert(const Table& table, uint64_t key,
        const GenericTuple& tuple, bool hasSucceeded /* = true */) {
    return executeInTransaction<ModificationResponse>(table, [this, &table, key, &tuple, hasSucceeded] () {
        mModified.insert(std::make_tuple(table.tableId(), key));
        return mProcessor.insert(mFiber, table.tableId(), key, table.record(), tuple, mSnapshot, hasSucceeded);
    });
}

std::shared_ptr<ModificationResponse> ClientTransaction::update(const Table& table, uint64_t key,
        const GenericTuple& tuple) {
    return executeInTransaction<ModificationResponse>(table, [this, &table, key, &tuple] () {
        mModified.insert(std::make_tuple(table.tableId(), key));
        return mProcessor.update(mFiber, table.tableId(), key, table.record(), tuple, mSnapshot);
    });
}

std::shared_ptr<ModificationResponse> ClientTransaction::remove(const Table& table, uint64_t key) {
    return executeInTransaction<ModificationResponse>(table, [this, &table, key] () {
        mModified.insert(std::make_tuple(table.tableId(), key));
        return mProcessor.remove(mFiber, table.tableId(), key, mSnapshot);
    });
}

std::shared_ptr<ScanResponse> ClientTransaction::scan(const Table& table, uint32_t queryLength, const char* query) {
    return executeInTransaction<ScanResponse>(table, [this, &table, queryLength, query] () {
        return mProcessor.scan(mFiber, table.tableId(), table.record(), queryLength, query, mSnapshot);
    });
}

void ClientTransaction::commit() {
    mModified.clear();
    mProcessor.commit(mSnapshot);
    mCommitted = true;
}

void ClientTransaction::abort() {
    if (!mModified.empty()) {
        rollbackModified();
    }
    commit();
}

void ClientTransaction::rollbackModified() {
    std::queue<std::shared_ptr<ModificationResponse>> responses;
    for (auto& modified : mModified) {
        auto revertFuture = mProcessor.revert(mFiber, std::get<0>(modified), std::get<1>(modified), mSnapshot);
        responses.emplace(std::move(revertFuture));
    }
    responses.back()->waitForResult();

    while (!responses.empty()) {
        auto response = std::move(responses.front());
        responses.pop();

        auto& ec = response->error();
        if (ec) {
            LOG_ERROR("Error while rolling back transaction [error = %1% %2%]", ec, ec.message());
            // TODO Handle this somehow
            continue;
        }

        auto succeeded = response->get();
        if (!succeeded) {
            LOG_ERROR("Revert did not succeed");
            // TODO Handle this somehow
            continue;
        }
    }
}

template <typename Response, typename Fun>
std::shared_ptr<Response> ClientTransaction::executeInTransaction(const Table& table, Fun fun) {
    checkTableType(table, TableType::TRANSACTIONAL);

    if (mCommitted) {
        throw std::logic_error("Transaction has already committed");
    }

    return fun();
}

ClientHandle::ClientHandle(ClientProcessor& processor, crossbow::infinio::Fiber& fiber)
        : mProcessor(processor),
          mFiber(fiber) {
}

std::shared_ptr<CreateTableResponse> ClientHandle::createTable(const crossbow::string& name, const Schema& schema) {
    return mProcessor.createTable(mFiber, name, schema);
}

std::shared_ptr<GetTableResponse> ClientHandle::getTable(const crossbow::string& name) {
    return mProcessor.getTable(mFiber, name);
}

ClientTransaction ClientHandle::startTransaction() {
    return mProcessor.start(mFiber);
}

std::shared_ptr<GetResponse> ClientHandle::get(const Table& table, uint64_t key) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = buildNonTransactionalSnapshot(std::numeric_limits<uint64_t>::max());
    return mProcessor.get(mFiber, table.tableId(), key, snapshot);
}

std::shared_ptr<ModificationResponse> ClientHandle::insert(const Table& table, uint64_t key, uint64_t version,
        const GenericTuple& tuple, bool hasSucceeded) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = buildNonTransactionalSnapshot(version);
    return mProcessor.insert(mFiber, table.tableId(), key, table.record(), tuple, snapshot, hasSucceeded);
}

std::shared_ptr<ModificationResponse> ClientHandle::update(const Table& table, uint64_t key, uint64_t version,
        const GenericTuple& tuple) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = buildNonTransactionalSnapshot(version);
    return mProcessor.update(mFiber, table.tableId(), key, table.record(), tuple, snapshot);
}

std::shared_ptr<ModificationResponse> ClientHandle::remove(const Table& table, uint64_t key, uint64_t version) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = buildNonTransactionalSnapshot(version);
    return mProcessor.remove(mFiber, table.tableId(), key, snapshot);
}

std::shared_ptr<ScanResponse> ClientHandle::scan(const Table& table, uint32_t queryLength, const char* query) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = buildNonTransactionalSnapshot(std::numeric_limits<uint64_t>::max());
    return mProcessor.scan(mFiber, table.tableId(), table.record(), queryLength, query, snapshot);
}

ClientProcessor::ClientProcessor(CommitManager& commitManager, crossbow::infinio::InfinibandService& service,
        crossbow::infinio::LocalMemoryRegion& scanRegion, const ClientConfig& config, uint64_t processorNum)
        : mCommitManager(commitManager),
          mScanRegion(scanRegion),
          mProcessor(service.createProcessor()),
          mTellStoreSocket(service.createSocket(*mProcessor)),
          mProcessorNum(processorNum),
          mTransactionCount(0x0u) {
    mTellStoreSocket.connect(config.server, config.port, mProcessorNum);
}

void ClientProcessor::execute(const std::function<void(ClientHandle&)>& fun) {
    ++mTransactionCount;
    mProcessor->executeFiber([this, fun] (crossbow::infinio::Fiber& fiber) {
        LOG_TRACE("Proc %1%] Execute client function", mProcessorNum);

        ClientHandle client(*this, fiber);
        fun(client);
    });
}

ClientManager::ClientManager(crossbow::infinio::InfinibandService& service, const ClientConfig& config) {
    auto data = mmap(nullptr, config.scanMemory, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
    if (data == MAP_FAILED) {
        // TODO Error handling
        std::terminate();
    }

    int flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
    mScanRegion = service.registerMemoryRegion(data, config.scanMemory, flags);

    mProcessor.reserve(config.numNetworkThreads);
    for (decltype(config.numNetworkThreads) i = 0; i < config.numNetworkThreads; ++i) {
        mProcessor.emplace_back(new ClientProcessor(mCommitManager, service, mScanRegion, config, i));
    }
}

void ClientManager::execute(std::function<void(ClientHandle&)> fun) {
    ClientProcessor* processor = nullptr;
    uint64_t minCount = std::numeric_limits<uint64_t>::max();
    for (auto& proc : mProcessor) {
        auto count = proc->transactionCount();
        if (minCount < count) {
            continue;
        }
        processor = proc.get();
        minCount = count;
    }
    LOG_ASSERT(processor != nullptr, "Found no processor");

    processor->execute(fun);
}

} // namespace store
} // namespace tell
