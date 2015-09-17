#include <tellstore/ClientManager.hpp>

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

ClientTransaction::ClientTransaction(BaseClientProcessor& processor, crossbow::infinio::Fiber& fiber, bool readOnly,
        std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot)
        : mProcessor(processor),
          mFiber(fiber),
          mSnapshot(std::move(snapshot)),
          mReadOnly(readOnly),
          mCommitted(false) {
    mModified.set_empty_key(std::make_tuple(0x0u, 0x0u));
}

ClientTransaction::~ClientTransaction() {
    if (mCommitted) {
        return;
    }

    try {
        abort();
    } catch (std::exception& e) {
        LOG_ERROR("T%1%] Exception caught while aborting transaction [error = %2%]", mSnapshot->version(), e.what());
    }
}

ClientTransaction::ClientTransaction(ClientTransaction&& other)
        : mProcessor(other.mProcessor),
          mFiber(other.mFiber),
          mSnapshot(std::move(other.mSnapshot)),
          mModified(std::move(other.mModified)),
          mReadOnly(other.mReadOnly),
          mCommitted(other.mCommitted) {
    other.mCommitted = true;
}

std::shared_ptr<GetResponse> ClientTransaction::get(const Table& table, uint64_t key) {
    checkTransaction(table, true);

    return mProcessor.get(mFiber, table.tableId(), key, *mSnapshot);
}

std::shared_ptr<ModificationResponse> ClientTransaction::insert(const Table& table, uint64_t key,
        const GenericTuple& tuple, bool hasSucceeded /* = true */) {
    checkTransaction(table, false);

    mModified.insert(std::make_tuple(table.tableId(), key));
    return mProcessor.insert(mFiber, table.tableId(), key, table.record(), tuple, *mSnapshot, hasSucceeded);
}

std::shared_ptr<ModificationResponse> ClientTransaction::update(const Table& table, uint64_t key,
        const GenericTuple& tuple) {
    checkTransaction(table, false);

    mModified.insert(std::make_tuple(table.tableId(), key));
    return mProcessor.update(mFiber, table.tableId(), key, table.record(), tuple, *mSnapshot);
}

std::shared_ptr<ModificationResponse> ClientTransaction::remove(const Table& table, uint64_t key) {
    checkTransaction(table, false);

    mModified.insert(std::make_tuple(table.tableId(), key));
    return mProcessor.remove(mFiber, table.tableId(), key, *mSnapshot);
}

std::vector<std::shared_ptr<ScanResponse>> ClientTransaction::scan(const Table& table, ScanMemoryManager& memoryManager,
        ScanQueryType queryType, uint32_t selectionLength, const char* selection, uint32_t queryLength,
        const char* query) {
    checkTransaction(table, true);

    return mProcessor.scan(mFiber, table.tableId(), table.record(), memoryManager, queryType, selectionLength,
            selection, queryLength, query, *mSnapshot);
}

void ClientTransaction::commit() {
    mModified.clear();
    mProcessor.commit(mFiber, *mSnapshot);
    mCommitted = true;
}

void ClientTransaction::abort() {
    if (!mModified.empty()) {
        LOG_ASSERT(!mReadOnly, "Modified elements even though transaction is read-only");
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

void ClientTransaction::checkTransaction(const Table& table, bool readOnly) {
    checkTableType(table, TableType::TRANSACTIONAL);

    if (mCommitted) {
        throw std::logic_error("Transaction has already committed");
    }
    if (mReadOnly && !readOnly) {
        throw std::logic_error("Transaction is read only");
    }
}

ClientHandle::ClientHandle(BaseClientProcessor& processor, crossbow::infinio::Fiber& fiber)
        : mProcessor(processor),
          mFiber(fiber) {
}

ClientTransaction ClientHandle::startTransaction(bool readOnly) {
    return mProcessor.start(mFiber, readOnly);
}

Table ClientHandle::createTable(const crossbow::string& name, Schema schema) {
    return mProcessor.createTable(mFiber, name, std::move(schema));
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

std::vector<std::shared_ptr<ScanResponse>> ClientHandle::scan(const Table& table, ScanMemoryManager& memoryManager,
        ScanQueryType queryType, uint32_t selectionLength, const char* selection, uint32_t queryLength,
        const char* query) {
    checkTableType(table, TableType::NON_TRANSACTIONAL);

    auto snapshot = nonTransactionalSnapshot(std::numeric_limits<uint64_t>::max());
    return mProcessor.scan(mFiber, table.tableId(), table.record(), memoryManager, queryType, selectionLength,
            selection, queryLength, query, *snapshot);
}

BaseClientProcessor::BaseClientProcessor(crossbow::infinio::InfinibandService& service, const ClientConfig& config,
        uint64_t processorNum)
        : mProcessor(service.createProcessor()),
          mCommitManagerSocket(service.createSocket(*mProcessor), config.maxPendingResponses),
          mProcessorNum(processorNum) {
    mCommitManagerSocket.connect(config.commitManager);

    mTellStoreSocket.reserve(config.tellStore.size());
    for (auto& ep : config.tellStore) {
        mTellStoreSocket.emplace_back(new ClientSocket(service.createSocket(*mProcessor), config.maxPendingResponses));
        mTellStoreSocket.back()->connect(ep, mProcessorNum);
    }
}

void BaseClientProcessor::shutdown() {
    if (mProcessor->threadId() == std::this_thread::get_id()) {
        throw std::runtime_error("Unable to shutdown from within the processing thread");
    }

    mCommitManagerSocket.shutdown();
    for (auto& socket : mTellStoreSocket) {
        socket->shutdown();
    }
}

ClientTransaction BaseClientProcessor::start(crossbow::infinio::Fiber& fiber, bool readOnly) {
    // TODO Return a transaction future?

    auto startResponse = mCommitManagerSocket.startTransaction(fiber, readOnly);
    if (!startResponse->waitForResult()) {
        throw std::system_error(startResponse->error());
    }

    return {*this, fiber, readOnly, startResponse->get()};
}

Table BaseClientProcessor::createTable(crossbow::infinio::Fiber& fiber, const crossbow::string& name, Schema schema) {
    // TODO Return a combined createTable future?
    std::vector<std::shared_ptr<CreateTableResponse>> requests;
    requests.reserve(mTellStoreSocket.size());
    for (auto& socket : mTellStoreSocket) {
        requests.emplace_back(socket->createTable(fiber, name, schema));
    }
    uint64_t tableId = 0u;
    for (auto& i : requests) {
        auto id = i->get();
        LOG_ASSERT(tableId == 0u || tableId == id, "Table IDs returned from shards do not match");
        tableId = id;
    }
    return Table(tableId, std::move(schema));
}

std::vector<std::shared_ptr<ScanResponse>> BaseClientProcessor::scan(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        const Record& record, ScanMemoryManager& memoryManager, ScanQueryType queryType, uint32_t selectionLength,
        const char* selection, uint32_t queryLength, const char* query,
        const commitmanager::SnapshotDescriptor& snapshot) {
    std::vector<std::shared_ptr<ScanResponse>> result;
    result.reserve(mTellStoreSocket.size());
    for (auto& socket : mTellStoreSocket) {
        result.emplace_back(socket->scan(fiber, tableId, record, memoryManager.acquire(), queryType, selectionLength,
                selection, queryLength, query, snapshot));
    }
    return result;
}

void BaseClientProcessor::commit(crossbow::infinio::Fiber& fiber, const commitmanager::SnapshotDescriptor& snapshot) {
    // TODO Return a commit future?

    auto commitResponse = mCommitManagerSocket.commitTransaction(fiber, snapshot.version());
    if (!commitResponse->waitForResult()) {
        throw std::system_error(commitResponse->error());
    }
    if (!commitResponse->get()) {
        throw std::runtime_error("Commit transaction did not succeed");
    }
}

} // namespace store
} // namespace tell
