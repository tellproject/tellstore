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

Transaction::Transaction(TransactionManager& manager, uint64_t id)
    : mManager(manager),
      mId(id),
      mContext(boost::context::make_fcontext(stackFromTransaction(this), STACK_SIZE, &Transaction::entry_fun)),
#if BOOST_VERSION >= 105600
      mReturnContext(nullptr),
#endif
      mOutstanding(0x0u) {
}

Transaction::~Transaction() {
    mManager.endTransaction(this);
}

bool Transaction::execute(std::function<void(Transaction&)> fun) {
    // Not Thread safe
    if (mContextFun) {
        return false;
    }
    mContextFun = std::move(fun);
    return resume();
}

bool Transaction::createTable(const crossbow::string& name, const Schema& schema, uint64_t& tableId,
        boost::system::error_code& ec) {
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.createTable(mId, name, schema, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.createTable(tableId, ec);
}

bool Transaction::get(uint64_t tableId, uint64_t key, size_t& size, const char*& data,
        const SnapshotDescriptor& snapshot, bool& isNewest, boost::system::error_code& ec) {
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.get(mId, tableId, key, snapshot, ec);
    if (ec) {
        return false;
    }
    wait();

    return mResponse.get(size, data, isNewest, ec);
}

void Transaction::insert(uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, boost::system::error_code& ec, bool* succeeded) {
    auto& con = mManager.mConnection;

    ++mOutstanding;
    con.insert(mId, tableId, key, size, data, snapshot, (succeeded == nullptr ? false : true), ec);
    if (ec) {
        return;
    }
    wait();

    mResponse.insert(succeeded, ec);
}

void Transaction::start() {
    while (true) {
        if (!mContextFun) {
            wait();
            continue;
        }

        try {
            LOG_INFO("Invoking transaction function");
            mContextFun(*this);
            mContextFun = std::function<void(Transaction&)>();
            wait();
        } catch (...) {
            LOG_FATAL("Exception triggered in ExecutionContext");
            std::terminate();
        }
    }
}

bool Transaction::resume() {
    tbb::spin_mutex::scoped_lock lock(mContextMutex);

    if (!mContextFun) {
        return false;
    }

#if BOOST_VERSION >= 105600
    LOG_ASSERT(!mReturnContext, "Resuming an already active context");
    auto res = boost::context::jump_fcontext(&mReturnContext, mContext, reinterpret_cast<intptr_t>(this));
    mReturnContext = nullptr;
#else
    auto res = boost::context::jump_fcontext(&mReturnContext, mContext, reinterpret_cast<intptr_t>(this));
#endif
    LOG_ASSERT(res == reinterpret_cast<intptr_t>(this), "Not returning from yield()");
    return true;
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

void TransactionManager::init(const ClientConfig& config, boost::system::error_code& ec, std::function<void ()> callback) {
    mCallback = std::move(callback);
    mConnection.connect(config.server, config.port, ec);

    // TODO Use a context/future to block for connect?
}

std::unique_ptr<Transaction> TransactionManager::startTransaction() {
    auto id = ++mTransactionId;

    std::unique_ptr<Transaction> transaction(Transaction::allocate(*this, id));

    tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, false);
    auto res = mTransactions.insert(std::make_pair(id, transaction.get())).second;
    LOG_ASSERT(res, "Unable to insert context for message ID %1%", id);

    return std::move(transaction);
}

void TransactionManager::onConnected(const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Failed to connect to server [errcode = %1% %2%]", ec, ec.message());
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
        // TODO Schedule this through the event loop?
        transaction->resume();
    }
}

void TransactionManager::endTransaction(Transaction* transaction) {
    tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, true);
    auto res = mTransactions.unsafe_erase(transaction->id());
    LOG_ASSERT(res == 1, "Unable to remove context for transaction ID %1%", transaction->id());
}

} // namespace store
} // namespace tell
