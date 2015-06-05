#pragma once

#include "ServerConnection.hpp"

#include <util/NonCopyable.hpp>
#include <util/GenericTuple.hpp>
#include <util/sparsehash/dense_hash_map>

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/string.hpp>

#include <boost/context/fcontext.hpp>
#include <boost/version.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <system_error>
#include <vector>

#include <jemalloc/jemalloc.h>

namespace tell {
namespace store {

class ClientConfig;
class Record;
class TransactionManager;

class Transaction : NonCopyable, NonMovable {
public:
    // TODO Overload new and delete?

    static constexpr size_t STACK_SIZE = 0x800000;

    static Transaction* allocate(TransactionProcessor& processor, uint64_t id, std::function<void(Transaction&)> fun) {
        void* data = je_malloc(STACK_SIZE + sizeof(Transaction));
        return new (data) Transaction(processor, id, std::move(fun));
    }

    static void destroy(Transaction* transaction) {
        transaction->~Transaction();
        je_free(transaction);
    }

    Transaction(TransactionProcessor& processor, uint64_t id, std::function<void(Transaction&)> fun);

    uint64_t id() const {
        return mId;
    }

    bool createTable(const crossbow::string& name, const Schema& schema, uint64_t& tableId, std::error_code& ec);

    bool getTableId(const crossbow::string& name, uint64_t& tableId, std::error_code& ec);

    bool get(uint64_t tableId, uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot,
            bool& isNewest, std::error_code& ec);

    bool getNewest(uint64_t tableId, uint64_t key, size_t& size, const char*& data, uint64_t& version,
            std::error_code& ec);

    bool update(uint64_t tableId, uint64_t key, const Record& record, const GenericTuple& tuple,
            const SnapshotDescriptor& snapshot, std::error_code& ec);

    bool update(uint64_t tableId, uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
            std::error_code& ec);

    void insert(uint64_t tableId, uint64_t key, const Record& record, const GenericTuple& tuple,
            const SnapshotDescriptor& snapshot, std::error_code& ec, bool* succeeded = nullptr);

    void insert(uint64_t tableId, uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
            std::error_code& ec, bool* succeeded = nullptr);

    bool remove(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot, std::error_code& ec);

    bool revert(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot, std::error_code& ec);

    size_t scan(uint64_t tableId, size_t size, const char* query, const SnapshotDescriptor& snapshot, std::error_code& ec);

private:
    friend class TransactionProcessor;

    static void entry_fun(intptr_t p);

    void start();

    void resume();

    void wait();

    bool setResponse(ServerConnection::Response response);

    void addScanTuple(uint16_t count);

    TransactionProcessor& mProcessor;

    uint64_t mId;

    std::function<void(Transaction&)> mFun;

#if BOOST_VERSION >= 105600
    boost::context::fcontext_t mContext;
#else
    boost::context::fcontext_t* mContext;
#endif

    boost::context::fcontext_t mReturnContext;

    uint32_t mOutstanding;
    ServerConnection::Response mResponse;
    uint64_t mTuplePending;
};

class TransactionProcessor {
public:
    TransactionProcessor(TransactionManager& manager, crossbow::infinio::InfinibandService& service, uint64_t num)
            : mManager(manager),
              mProcessorNumber(num),
              mConnection(service.createSocket(mProcessorNumber), *this),
              mConnected(false),
              mTransactionCount(0),
              mTransactionId(0x0u),
              mScanId(0x0u) {
        mTransactions.set_empty_key(0x0u);
        mTransactions.set_deleted_key(std::numeric_limits<uint64_t>::max());

        mScans.set_empty_key(0x0u);
        mScans.set_deleted_key(std::numeric_limits<uint16_t>::max());
    }

    ~TransactionProcessor();

    void init(const ClientConfig& config, std::error_code& ec);

    uint64_t transactionCount() const {
        return mTransactionCount;
    }

    void executeTransaction(std::function<void(Transaction&)> fun);

    void endTransaction(uint64_t id);

    uint16_t startScan(Transaction* transaction, uint64_t tableId, size_t size, const char* query,
            const SnapshotDescriptor& snapshot, std::error_code& ec);

    void endScan(uint16_t id);

private:
    friend class ServerConnection;
    friend class Transaction;

    void onConnected(const std::error_code& ec);

    void handleResponse(uint64_t id, ServerConnection::Response response);

    void handleScanProgress(uint16_t id, uint16_t tupleCount);

    TransactionManager& mManager;

    uint64_t mProcessorNumber;

    ServerConnection mConnection;

    bool mConnected;

    std::atomic<uint64_t> mTransactionCount;
    std::atomic<uint64_t> mTransactionId;
    google::dense_hash_map<uint64_t, Transaction*> mTransactions;

    uint16_t mScanId;
    google::dense_hash_map<uint16_t, Transaction*> mScans;
};

class TransactionManager {
public:
    TransactionManager(crossbow::infinio::InfinibandService& service);

    ~TransactionManager();

    void init(const ClientConfig& config, std::error_code& ec);

    void executeTransaction(std::function<void(Transaction&)> fun);

private:
    friend class TransactionProcessor;

    crossbow::infinio::LocalMemoryRegion& scanRegion() {
        return mScanRegion;
    }

    crossbow::infinio::LocalMemoryRegion mScanRegion;

    std::vector<TransactionProcessor*> mProcessor;
};

} // namespace store
} // namespace tell
