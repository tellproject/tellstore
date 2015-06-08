#include "TransactionManager.hpp"

#include "ClientConfig.hpp"

#include <util/Logging.hpp>

#include <sys/mman.h>

namespace tell {
namespace store {

namespace {

void* stackFromTransaction(Transaction* transaction) {
    return reinterpret_cast<char*>(transaction) + Transaction::STACK_SIZE + sizeof(Transaction);
}

/**
 * @brief Memory reserved for scans
 */
const size_t gScanTotalMemory = 0x80000000ull;

} // anonymous namespace

void Transaction::entry_fun(intptr_t p) {
    auto self = reinterpret_cast<Transaction*>(p);
    self->start();
    assert(false); // never returns
}

Transaction::Transaction(TransactionProcessor& processor, uint64_t id, std::function<void(Transaction&)> fun)
    : mProcessor(processor),
      mId(id),
      mFun(std::move(fun)),
      mContext(boost::context::make_fcontext(stackFromTransaction(this), STACK_SIZE, &Transaction::entry_fun)),
#if BOOST_VERSION >= 105600
      mReturnContext(nullptr),
#endif
      mOutstanding(0x0u),
      mTuplePending(0x0u) {
}

bool Transaction::createTable(const crossbow::string& name, const Schema& schema, uint64_t& tableId,
        std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.createTable(mId, name, schema, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.createTable(tableId, ec);
}

bool Transaction::getTableId(const crossbow::string& name, uint64_t& tableId, std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.getTableId(mId, name, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.getTableId(tableId, ec);
}

bool Transaction::get(uint64_t tableId, uint64_t key, size_t& size, const char*& data,
        const SnapshotDescriptor& snapshot, bool& isNewest, std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.get(mId, tableId, key, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    uint64_t version = 0;
    return mResponse.get(size, data, version, isNewest, ec);
}

bool Transaction::getNewest(uint64_t tableId, uint64_t key, size_t& size, const char*& data, uint64_t& version,
        std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.getNewest(mId, tableId, key, ec);
    if (ec) {
        return false;
    }
    wait();

    bool isNewest = false;
    return mResponse.get(size, data, version, isNewest, ec);
}

bool Transaction::update(uint64_t tableId, uint64_t key, const Record& record, const GenericTuple& tuple,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.update(mId, tableId, key, record, tuple, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.modification(ec);
}

bool Transaction::update(uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.update(mId, tableId, key, size, data, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.modification(ec);
}

void Transaction::insert(uint64_t tableId, uint64_t key, const Record& record, const GenericTuple& tuple,
        const SnapshotDescriptor& snapshot, std::error_code& ec, bool* succeeded) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.insert(mId, tableId, key, record, tuple, snapshot, (succeeded == nullptr ? false : true), ec);
    if (ec) {
        if (succeeded) {
            *succeeded = false;
        }
        return;
    }
    wait();

    auto s = mResponse.modification(ec);
    if (succeeded) {
        *succeeded = s;
    }
}

void Transaction::insert(uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, std::error_code& ec, bool* succeeded) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.insert(mId, tableId, key, size, data, snapshot, (succeeded == nullptr ? false : true), ec);
    if (ec) {
        if (succeeded) {
            *succeeded = false;
        }
        return;
    }
    wait();

    auto s = mResponse.modification(ec);
    if (succeeded) {
        *succeeded = s;
    }
}

bool Transaction::remove(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.remove(mId, tableId, key, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.modification(ec);
}

bool Transaction::revert(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto& con = mProcessor.mConnection;

    mResponse.reset();
    ++mOutstanding;
    con.revert(mId, tableId, key, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.modification(ec);
}

size_t Transaction::scan(uint64_t tableId, size_t size, const char* query, const SnapshotDescriptor& snapshot, std::error_code& ec) {
    mTuplePending = 0x0u;
    mResponse.reset();
    ++mOutstanding;
    auto id = mProcessor.startScan(this, tableId, size, query, snapshot, ec);
    if (ec) {
        return 0x0u;
    }

    while (true) {
        wait();
        if (mResponse.isSet()) {
            uint16_t scanId;
            mResponse.scan(scanId, ec);
            break;
        }

        // TODO Process the tuples somehow
    }
    mProcessor.endScan(id);

    return mTuplePending;
}

void Transaction::start() {
    try {
        LOG_TRACE("Invoking transaction function");
        mFun(*this);
    } catch (std::exception& e) {
        LOG_ERROR("Exception triggered in transaction function [error = %1%]", e.what());
    } catch (...) {
        LOG_ERROR("Exception triggered in transaction function");
    }
    mProcessor.endTransaction(mId);
}

void Transaction::resume() {
#if BOOST_VERSION >= 105600
    LOG_ASSERT(!mReturnContext, "Resuming an already active context");
    auto res = boost::context::jump_fcontext(&mReturnContext, mContext, reinterpret_cast<intptr_t>(this));
    mReturnContext = nullptr;
#else
    auto res = boost::context::jump_fcontext(&mReturnContext, mContext, reinterpret_cast<intptr_t>(this));
#endif
    LOG_ASSERT(res == reinterpret_cast<intptr_t>(this), "Not returning from yield()");
}

void Transaction::wait() {
#if BOOST_VERSION >= 105600
    LOG_ASSERT(mReturnContext, "Not waiting from active context");
    auto res = boost::context::jump_fcontext(&mContext, mReturnContext, reinterpret_cast<intptr_t>(this));
#else
    auto res = boost::context::jump_fcontext(mContext, &mReturnContext, reinterpret_cast<intptr_t>(this));
#endif
    LOG_ASSERT(res == reinterpret_cast<intptr_t>(this), "Not returning from resume()");
}

bool Transaction::setResponse(ServerConnection::Response response) {
    LOG_ASSERT(mOutstanding == 1, "Only one outstanding response supported at the moment");
    mResponse = std::move(response);
    return (--mOutstanding == 0);
}

void Transaction::addScanTuple(uint16_t count) {
    mTuplePending += count;
}

TransactionProcessor::~TransactionProcessor() {
    // TODO Abort all transactions
}

void TransactionProcessor::init(const ClientConfig& config, std::error_code& ec) {
    mConnection.connect(config.server, config.port, mProcessorNumber, ec);
}

void TransactionProcessor::executeTransaction(std::function<void(Transaction&)> fun) {
    ++mTransactionCount;

    auto id = ++mTransactionId;

    LOG_DEBUG("Proc %1% TID %2%] Starting transaction", mProcessorNumber, id);
    auto transaction = Transaction::allocate(*this, id, std::move(fun));

    std::error_code ec;
    mConnection.execute([this, transaction] () {
        auto res = mTransactions.insert(std::make_pair(transaction->id(), transaction)).second;
        LOG_ASSERT(res, "Unable to insert transaction");
        if (mConnected) {
            transaction->resume();
        }
    }, ec);
    if (ec) {
        LOG_ERROR("Proc %1% TID %2%] Failure starting transaction [error = %3% %4%]", mProcessorNumber, id, ec,
                ec.message());
        je_free(transaction);
    }
}

void TransactionProcessor::endTransaction(uint64_t id) {
    LOG_DEBUG("Proc %1% TID %2%] Ending transaction", mProcessorNumber, id);

    Transaction* transaction;
    {
        auto i = mTransactions.find(id);
        if (i == mTransactions.end()) {
            LOG_DEBUG("Proc %1% ID %2%] Transaction not found", mProcessorNumber, id);
            return;
        }
        transaction = i->second;
        mTransactions.erase(i);
    }

    // endTransaction() is called from the transaction context itself, so we can not free the memory associated with the
    // context here - defer deletion to the event loop
    std::error_code ec;
    mConnection.execute([transaction] () {
        transaction->~Transaction();
        je_free(transaction);
    }, ec);
    if (ec) {
        LOG_ERROR("Proc %1% TID %2%] Failure releasing transaction [error = %3% %4%]", mProcessorNumber, id, ec,
                ec.message());
        return;
    }

    transaction->wait();
}

uint16_t TransactionProcessor::startScan(Transaction* transaction, uint64_t tableId, size_t size, const char* query,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto id = ++mScanId;
    mScans.insert(std::make_pair(id, transaction));

    mConnection.scan(transaction->id(), tableId, id, mManager.scanRegion(), size, query, snapshot, ec);

    return id;
}

void TransactionProcessor::endScan(uint16_t id) {
    auto i = mScans.find(id);
    if (i == mScans.end()) {
        LOG_DEBUG("Proc %1% Scan %2%] Scan not found", mProcessorNumber, id);
        return;
    }
    mScans.erase(i);
}

void TransactionProcessor::onConnected(const std::error_code& ec) {
    if (ec) {
        LOG_ERROR("Proc %1%] Failed to connect to server [error = %2% %3%]", mProcessorNumber, ec, ec.message());
        return;
    }
    LOG_DEBUG("Proc %1%] Connected to server", mProcessorNumber);

    mConnected = true;

    // Start all transactions that have been waiting for the connect
    for (auto trans : mTransactions) {
        trans.second->resume();
    }
}

void TransactionProcessor::handleResponse(uint64_t id, ServerConnection::Response response) {
    Transaction* transaction;
    {
        auto i = mTransactions.find(id);
        if (i == mTransactions.end()) {
            LOG_WARN("Proc %1% ID %2%] Transaction not found", mProcessorNumber, id);
            // TODO Handle this correctly
            return;
        }
        transaction = i->second;
    }

    if (transaction->setResponse(std::move(response))) {
        transaction->resume();
    }
}

void TransactionProcessor::handleScanProgress(uint16_t id, uint16_t tupleCount) {
    Transaction* transaction;
    {
        auto i = mScans.find(id);
        if (i == mScans.end()) {
            LOG_WARN("Proc %1% Scan %2%] Transaction not found", mProcessorNumber, id);
            // TODO Handle this correctly
            return;
        }
        transaction = i->second;
    }
    transaction->addScanTuple(tupleCount);
    transaction->resume();
}

TransactionManager::TransactionManager(crossbow::infinio::InfinibandService& service) {
    auto data = mmap(nullptr, gScanTotalMemory, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
    if (data == MAP_FAILED) {
        // TODO Error handling
        std::terminate();
    }

    int flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
    mScanRegion = service.registerMemoryRegion(data, gScanTotalMemory, flags);

    auto numThreads = service.limits().contextThreads;
    mProcessor.reserve(numThreads);
    for (auto i = 0; i < numThreads; ++i) {
        mProcessor.emplace_back(new TransactionProcessor(*this, service, i));
    }
}

TransactionManager::~TransactionManager() {
    for (auto proc : mProcessor) {
        delete proc;
    }
}

void TransactionManager::init(const ClientConfig& config, std::error_code& ec) {
    for (auto proc : mProcessor) {
        proc->init(config, ec);
        if (ec) {
            return;
        }
    }
}

void TransactionManager::executeTransaction(std::function<void(Transaction&)> fun) {
    TransactionProcessor* processor = nullptr;
    uint64_t minCount = std::numeric_limits<uint64_t>::max();
    for (auto proc : mProcessor) {
        auto count = proc->transactionCount();
        if (minCount < count) {
            continue;
        }
        processor = proc;
        minCount = count;
    }
    LOG_ASSERT(processor != nullptr, "Found no processor");

    processor->executeTransaction(std::move(fun));
}

} // namespace store
} // namespace tell
