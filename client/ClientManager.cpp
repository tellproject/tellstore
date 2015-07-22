#include <tellstore/ClientManager.hpp>

#include <tellstore/ClientConfig.hpp>

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

std::unique_ptr<commitmanager::SnapshotDescriptor> nonTransactionalSnapshot(uint64_t baseVersion) {
    auto version = (baseVersion == std::numeric_limits<uint64_t>::max() ? baseVersion : baseVersion + 1);
    commitmanager::SnapshotDescriptor::BlockType descriptor = 0x0u;
    return commitmanager::SnapshotDescriptor::create(0x0u, baseVersion, version,
            reinterpret_cast<const char*>(&descriptor));
}

} // anonymous namespace

ClientTransaction::ClientTransaction(ClientProcessor& processor, crossbow::infinio::Fiber& fiber,
        std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot)
        : mProcessor(processor),
          mFiber(fiber),
          mSnapshot(std::move(snapshot)),
          mCommitted(false) {
    mModified.set_empty_key(std::make_tuple(0x0u, 0x0u));
}

ClientTransaction::~ClientTransaction() {
    if (!mCommitted) {
        try {
            abort();
        } catch (std::exception& e) {
            LOG_ERROR("Exception caught while aborting transaction [error = %1%]", e.what());
        }
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
        return mProcessor.get(mFiber, table.tableId(), key, *mSnapshot);
    });
}

std::shared_ptr<ModificationResponse> ClientTransaction::insert(const Table& table, uint64_t key,
        const GenericTuple& tuple, bool hasSucceeded /* = true */) {
    return executeInTransaction<ModificationResponse>(table, [this, &table, key, &tuple, hasSucceeded] () {
        mModified.insert(std::make_tuple(table.tableId(), key));
        return mProcessor.insert(mFiber, table.tableId(), key, table.record(), tuple, *mSnapshot, hasSucceeded);
    });
}

std::shared_ptr<ModificationResponse> ClientTransaction::update(const Table& table, uint64_t key,
        const GenericTuple& tuple) {
    return executeInTransaction<ModificationResponse>(table, [this, &table, key, &tuple] () {
        mModified.insert(std::make_tuple(table.tableId(), key));
        return mProcessor.update(mFiber, table.tableId(), key, table.record(), tuple, *mSnapshot);
    });
}

std::shared_ptr<ModificationResponse> ClientTransaction::remove(const Table& table, uint64_t key) {
    return executeInTransaction<ModificationResponse>(table, [this, &table, key] () {
        mModified.insert(std::make_tuple(table.tableId(), key));
        return mProcessor.remove(mFiber, table.tableId(), key, *mSnapshot);
    });
}

std::shared_ptr<ScanResponse> ClientTransaction::scan(const Table& table, uint32_t queryLength, const char* query) {
    return executeInTransaction<ScanResponse>(table, [this, &table, queryLength, query] () {
        return mProcessor.scan(mFiber, table.tableId(), table.record(), queryLength, query, *mSnapshot);
    });
}

void ClientTransaction::commit() {
    mModified.clear();
    mProcessor.commit(mFiber, *mSnapshot);
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
        auto revertResponse = mProcessor.revert(mFiber, std::get<0>(modified), std::get<1>(modified), *mSnapshot);
        responses.emplace(std::move(revertResponse));
    }
    responses.back()->waitForResult();

    while (!responses.empty()) {
        auto revertResponse = std::move(responses.front());
        responses.pop();

        if (!revertResponse->waitForResult()) {
            throw std::system_error(revertResponse->error());
        }
        if (!revertResponse->get()) {
            throw std::logic_error("Revert did not succeed");
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

ClientTransaction ClientHandle::startTransaction() {
    return mProcessor.start(mFiber);
}

std::shared_ptr<CreateTableResponse> ClientHandle::createTable(const crossbow::string& name, const Schema& schema) {
    return mProcessor.createTable(mFiber, name, schema);
}

std::shared_ptr<GetTableResponse> ClientHandle::getTable(const crossbow::string& name) {
    return mProcessor.getTable(mFiber, name);
}

std::shared_ptr<GetResponse> ClientHandle::get(const Table& table, uint64_t key) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = nonTransactionalSnapshot(std::numeric_limits<uint64_t>::max());
    return mProcessor.get(mFiber, table.tableId(), key, *snapshot);
}

std::shared_ptr<ModificationResponse> ClientHandle::insert(const Table& table, uint64_t key, uint64_t version,
        const GenericTuple& tuple, bool hasSucceeded) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = nonTransactionalSnapshot(version);
    return mProcessor.insert(mFiber, table.tableId(), key, table.record(), tuple, *snapshot, hasSucceeded);
}

std::shared_ptr<ModificationResponse> ClientHandle::update(const Table& table, uint64_t key, uint64_t version,
        const GenericTuple& tuple) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = nonTransactionalSnapshot(version);
    return mProcessor.update(mFiber, table.tableId(), key, table.record(), tuple, *snapshot);
}

std::shared_ptr<ModificationResponse> ClientHandle::remove(const Table& table, uint64_t key, uint64_t version) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = nonTransactionalSnapshot(version);
    return mProcessor.remove(mFiber, table.tableId(), key, *snapshot);
}

std::shared_ptr<ScanResponse> ClientHandle::scan(const Table& table, uint32_t queryLength, const char* query) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = nonTransactionalSnapshot(std::numeric_limits<uint64_t>::max());
    return mProcessor.scan(mFiber, table.tableId(), table.record(), queryLength, query, *snapshot);
}

ClientProcessor::ClientProcessor(crossbow::infinio::InfinibandService& service,
        crossbow::infinio::AllocatedMemoryRegion& scanRegion, const crossbow::infinio::Endpoint& commitManager,
        const crossbow::infinio::Endpoint& tellStore, uint64_t processorNum)
        : mScanRegion(scanRegion),
          mProcessor(service.createProcessor()),
          mCommitManagerSocket(service.createSocket(*mProcessor)),
          mTellStoreSocket(service.createSocket(*mProcessor)),
          mProcessorNum(processorNum),
          mTransactionCount(0x0u) {
    mCommitManagerSocket.connect(commitManager);
    mTellStoreSocket.connect(tellStore, mProcessorNum);
}

void ClientProcessor::execute(const std::function<void(ClientHandle&)>& fun) {
    ++mTransactionCount;
    mProcessor->executeFiber([this, fun] (crossbow::infinio::Fiber& fiber) {
        LOG_TRACE("Proc %1%] Execute client function", mProcessorNum);

        ClientHandle client(*this, fiber);
        fun(client);
    });
}

ClientTransaction ClientProcessor::start(crossbow::infinio::Fiber& fiber) {
    // TODO Return a transaction future?

    auto startResponse = mCommitManagerSocket.startTransaction(fiber);
    if (!startResponse->waitForResult()) {
        throw std::system_error(startResponse->error());
    }

    return {*this, fiber, startResponse->get()};
}

void ClientProcessor::commit(crossbow::infinio::Fiber& fiber, const commitmanager::SnapshotDescriptor& snapshot) {
    // TODO Return a commit future?

    auto commitResponse = mCommitManagerSocket.commitTransaction(fiber, snapshot.version());
    if (!commitResponse->waitForResult()) {
        throw std::system_error(commitResponse->error());
    }
    if (!commitResponse->get()) {
        throw std::runtime_error("Commit transaction did not succeed");
    }
}

ClientManager::ClientManager(crossbow::infinio::InfinibandService& service, const ClientConfig& config)
        : mScanRegion(service.allocateMemoryRegion(config.scanMemory,
                IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE)) {
    mProcessor.reserve(config.numNetworkThreads);
    for (decltype(config.numNetworkThreads) i = 0; i < config.numNetworkThreads; ++i) {
        mProcessor.emplace_back(new ClientProcessor(service, mScanRegion, config.commitManager, config.tellStore, i));
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
