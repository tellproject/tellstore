#include "TransactionManager.hpp"

#include "ClientConfig.hpp"

#include <util/Logging.hpp>

namespace tell {
namespace store {

namespace {

void* stackFromTransaction(Transaction* transaction) {
    return reinterpret_cast<char*>(transaction) + Transaction::STACK_SIZE + sizeof(Transaction);
}

} // anonymous namespace

void Transaction::entry_fun(intptr_t p) {
    auto self = reinterpret_cast<Transaction*>(p);
    self->start();
    assert(false); // never returns
}

Transaction::Transaction(TransactionManager& manager, uint64_t id, std::function<void(Transaction&)> fun)
    : mManager(manager),
      mId(id),
      mFun(std::move(fun)),
      mContext(boost::context::make_fcontext(stackFromTransaction(this), STACK_SIZE, &Transaction::entry_fun)),
#if BOOST_VERSION >= 105600
      mReturnContext(nullptr),
#endif
      mOutstanding(0x0u) {
}

bool Transaction::createTable(const crossbow::string& name, const Schema& schema, uint64_t& tableId,
        std::error_code& ec) {
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.createTable(mId, name, schema, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.createTable(tableId, ec);
}

bool Transaction::getTableId(const crossbow::string& name, uint64_t& tableId, std::error_code& ec) {
    auto& con = mManager.mConnection;

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
    auto& con = mManager.mConnection;

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
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.getNewest(mId, tableId, key, ec);
    if (ec) {
        return false;
    }
    wait();

    bool isNewest = false;
    return mResponse.get(size, data, version, isNewest, ec);
}

bool Transaction::update(uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.update(mId, tableId, key, size, data, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.modification(ec);
}

void Transaction::insert(uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, std::error_code& ec, bool* succeeded) {
    auto& con = mManager.mConnection;

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
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.remove(mId, tableId, key, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.modification(ec);
}

bool Transaction::revert(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.revert(mId, tableId, key, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.modification(ec);
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
    mManager.endTransaction(mId);
}

void Transaction::resume() {
    tbb::spin_mutex::scoped_lock lock(mContextMutex);

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

TransactionManager::~TransactionManager() {
    // TODO Abort all transactions
}

void TransactionManager::init(const ClientConfig& config, std::error_code& ec, std::function<void()> callback) {
    mCallback = std::move(callback);
    mConnection.connect(config.server, config.port, ec);

    // TODO Use a context/future to block for connect?
}

void TransactionManager::executeTransaction(std::function<void(Transaction&)> fun) {
    auto id = ++mTransactionId;

    LOG_DEBUG("Starting transaction %1%", id);
    auto transaction = Transaction::allocate(*this, id, std::move(fun));

    tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, false);
    auto res = mTransactions.insert(std::make_pair(id, transaction)).second;
    LOG_ASSERT(res, "Unable to insert context for message ID %1%", id);

    std::error_code ec;
    mConnection.execute([transaction] () {
        transaction->resume();
    }, ec);

    // TODO Error handling
}

void TransactionManager::onConnected(const std::error_code& ec) {
    if (ec) {
        LOG_ERROR("Failed to connect to server [error = %1% %2%]", ec, ec.message());
        return;
    }
    LOG_DEBUG("Connected to server");
    mCallback();
}

void TransactionManager::handleResponse(uint64_t id, ServerConnection::Response response) {
    Transaction* transaction;
    {
        tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, false);
        auto i = mTransactions.find(id);
        if (i == mTransactions.end()) {
            LOG_WARN("Transaction for ID %1% not found", id);
            // TODO Handle this correctly
            return;
        }
        transaction = i->second;
    }

    if (transaction->setResponse(std::move(response))) {
        transaction->resume();
    }
}

void TransactionManager::endTransaction(uint64_t id) {
    LOG_DEBUG("Ending transaction %1%", id);

    Transaction* transaction = nullptr;
    {
        tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, true);
        auto i = mTransactions.find(id);
        if (i == mTransactions.end()) {
            LOG_DEBUG("Transaction with ID %1% not found", id);
            return;
        }
        transaction = i->second;
        mTransactions.unsafe_erase(i);
    }

    // endTransaction() is called from the transaction context itself, so we can not free the memory associated with the
    // context here - defer deletion to the event loop
    std::error_code ec;
    mConnection.execute([transaction] () {
        je_free(transaction);
    }, ec);
    // TODO Error handling

    transaction->wait();
}

} // namespace store
} // namespace tell
