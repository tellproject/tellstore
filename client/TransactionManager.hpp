#pragma once

#include "ServerConnection.hpp"

#include <util/NonCopyable.hpp>

#include <crossbow/string.hpp>

#include <tbb/concurrent_unordered_map.h>
#include <tbb/queuing_rw_mutex.h>
#include <tbb/spin_mutex.h>

#include <boost/context/fcontext.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/version.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <system_error>

#include <jemalloc/jemalloc.h>

namespace tell {
namespace store {

class ClientConfig;
class TransactionManager;

class Transaction : NonCopyable, NonMovable {
public:
    // TODO Overload new and delete?

    static constexpr size_t STACK_SIZE = 0x800000;

    static Transaction* allocate(TransactionManager& manager, uint64_t id, std::function<void(Transaction&)> fun) {
        void* data = je_malloc(STACK_SIZE + sizeof(Transaction));
        return new (data) Transaction(manager, id, std::move(fun));
    }

    static void destroy(Transaction* transaction) {
        transaction->~Transaction();
        je_free(transaction);
    }

    Transaction(TransactionManager& manager, uint64_t id, std::function<void(Transaction&)> fun);

    uint64_t id() const {
        return mId;
    }

    bool createTable(const crossbow::string& name, const Schema& schema, uint64_t& tableId, std::error_code& ec);

    bool getTableId(const crossbow::string& name, uint64_t& tableId, std::error_code& ec);

    bool get(uint64_t tableId, uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot,
            bool& isNewest, std::error_code& ec);

    bool getNewest(uint64_t tableId, uint64_t key, size_t& size, const char*& data, uint64_t& version,
            std::error_code& ec);

    bool update(uint64_t tableId, uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

    void insert(uint64_t tableId, uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
            std::error_code& ec, bool* succeeded = nullptr);

    bool remove(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot, std::error_code& ec);

    bool revert(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot, std::error_code& ec);

private:
    friend class TransactionManager;

    static void entry_fun(intptr_t p);

    void start();

    void resume();

    void wait();

    bool setResponse(ServerConnection::Response response);

    TransactionManager& mManager;

    uint64_t mId;

    std::function<void(Transaction&)> mFun;

    /// We expect transactions to yield immediately after posting the last send so the lock is only for safety reasons
    /// and will not often be contended.
    tbb::spin_mutex mContextMutex;

#if BOOST_VERSION >= 105600
    boost::context::fcontext_t mContext;
#else
    boost::context::fcontext_t* mContext;
#endif

    boost::context::fcontext_t mReturnContext;

    std::atomic<uint32_t> mOutstanding;
    ServerConnection::Response mResponse;
};

class TransactionManager {
public:
    TransactionManager(crossbow::infinio::InfinibandService& service)
            : mTransactionId(0x0u),
              mConnection(service, *this) {
    }

    ~TransactionManager();

    void init(const ClientConfig& config, std::error_code& ec, std::function<void()> callback);

    void executeTransaction(std::function<void(Transaction&)> fun);

private:
    friend class Transaction;
    friend class ServerConnection;

    void onConnected(const std::error_code& ec);

    void handleResponse(uint64_t id, ServerConnection::Response response);

    void endTransaction(uint64_t id);

    std::atomic<uint64_t> mTransactionId;

    ServerConnection mConnection;

    tbb::queuing_rw_mutex mTransactionsMutex;

    tbb::concurrent_unordered_map<uint64_t, Transaction*> mTransactions;


    std::function<void()> mCallback;
};

} // namespace store
} // namespace tell
