#pragma once

#include <tellstore/ClientSocket.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/ScanMemory.hpp>
#include <tellstore/Table.hpp>

#include <commitmanager/ClientSocket.hpp>
#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/Fiber.hpp>
#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

#include <sparsehash/dense_hash_set>

#include <boost/functional/hash.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <system_error>
#include <tuple>
#include <vector>

namespace tell {
namespace store {

struct ClientConfig;
class ClientProcessor;
class Record;

/**
 * @brief Class representing a TellStore transaction
 */
class ClientTransaction : crossbow::non_copyable {
public:
    ClientTransaction(ClientProcessor& processor, crossbow::infinio::Fiber& fiber,
            std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot);

    ~ClientTransaction();

    ClientTransaction(ClientTransaction&& other);

    uint64_t version() const {
        return mSnapshot->version();
    }

    std::shared_ptr<GetResponse> get(const Table& table, uint64_t key);

    std::shared_ptr<ModificationResponse> insert(const Table& table, uint64_t key, const GenericTuple& tuple,
            bool hasSucceeded = true);

    std::shared_ptr<ModificationResponse> update(const Table& table, uint64_t key, const GenericTuple& tuple);

    std::shared_ptr<ModificationResponse> remove(const Table& table, uint64_t key);

    std::vector<std::shared_ptr<ScanResponse>> scan(const Table& table, ScanMemoryManager& memoryManager,
            ScanQueryType queryType, uint32_t selectionLength, const char* selection, uint32_t queryLength,
            const char* query);

    void commit();

    void abort();

private:
    struct ModifiedHasher {
        size_t operator()(const std::tuple<uint64_t, uint64_t>& value) const {
            auto hash = std::hash<uint64_t>()(std::get<0>(value));
            boost::hash_combine(hash, std::hash<uint64_t>()(std::get<1>(value)));
            return hash;
        }
    };

    void rollbackModified();

    void checkTransaction(const Table& table);

    ClientProcessor& mProcessor;
    crossbow::infinio::Fiber& mFiber;

    std::unique_ptr<commitmanager::SnapshotDescriptor> mSnapshot;

    google::dense_hash_set<std::tuple<uint64_t, uint64_t>, ModifiedHasher> mModified;

    bool mCommitted;
};

/**
 * @brief Class to interact with the TellStore from within a fiber
 */
class ClientHandle : crossbow::non_copyable, crossbow::non_movable {
public:
    ClientHandle(ClientProcessor& processor, crossbow::infinio::Fiber& fiber);

    crossbow::infinio::Fiber& fiber() {
        return mFiber;
    }

    ClientTransaction startTransaction();

    Table createTable(const crossbow::string& name, Schema schema);

    std::shared_ptr<GetTableResponse> getTable(const crossbow::string& name);

    std::shared_ptr<GetResponse> get(const Table& table, uint64_t key);

    std::shared_ptr<ModificationResponse> insert(const Table& table, uint64_t key, uint64_t version,
            const GenericTuple& tuple, bool hasSucceeded = true);

    std::shared_ptr<ModificationResponse> update(const Table& table, uint64_t key, uint64_t version,
            const GenericTuple& tuple);

    std::shared_ptr<ModificationResponse> remove(const Table& table, uint64_t key, uint64_t version);

    std::vector<std::shared_ptr<ScanResponse>> scan(const Table& table, ScanMemoryManager& memoryManager,
            ScanQueryType queryType, uint32_t selectionLength, const char* selection, uint32_t queryLength,
            const char* query);

private:
    ClientProcessor& mProcessor;
    crossbow::infinio::Fiber& mFiber;
};

/**
 * @brief Class managing all running TellStore fibers
 */
class ClientProcessor : crossbow::non_copyable, crossbow::non_movable {
public:
    ClientProcessor(crossbow::infinio::InfinibandService& service, const crossbow::infinio::Endpoint& commitManager,
            const std::vector<crossbow::infinio::Endpoint>& tellStore, size_t maxPendingResponses,
            uint64_t processorNum);

    void shutdown();

    uint64_t transactionCount() const {
        return mTransactionCount.load();
    }

    void execute(const std::function<void(ClientHandle&)>& fun);

private:
    friend class ClientHandle;
    friend class ClientTransaction;

    /**
     * @brief The socket associated with the shard for the given table and key
     */
    store::ClientSocket* shard(uint64_t tableId, uint64_t key) {
        auto hash = std::hash<uint64_t>()(tableId);
        boost::hash_combine(hash, std::hash<uint64_t>()(key));
        return mTellStoreSocket.at(hash % mTellStoreSocket.size()).get();
    }

    ClientTransaction start(crossbow::infinio::Fiber& fiber);

    Table createTable(crossbow::infinio::Fiber& fiber, const crossbow::string& name, Schema schema);

    std::shared_ptr<GetTableResponse> getTable(crossbow::infinio::Fiber& fiber, const crossbow::string& name) {
        return mTellStoreSocket.at(0)->getTable(fiber, name);
    }

    std::shared_ptr<GetResponse> get(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
            const commitmanager::SnapshotDescriptor& snapshot) {
        return shard(tableId, key)->get(fiber, tableId, key, snapshot);
    }

    std::shared_ptr<ModificationResponse> insert(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
            const Record& record, const GenericTuple& tuple, const commitmanager::SnapshotDescriptor& snapshot,
            bool hasSucceeded) {
        return shard(tableId, key)->insert(fiber, tableId, key, record, tuple, snapshot, hasSucceeded);
    }

    std::shared_ptr<ModificationResponse> update(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
            const Record& record, const GenericTuple& tuple, const commitmanager::SnapshotDescriptor& snapshot) {
        return shard(tableId, key)->update(fiber, tableId, key, record, tuple, snapshot);
    }

    std::shared_ptr<ModificationResponse> remove(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
            const commitmanager::SnapshotDescriptor& snapshot) {
        return shard(tableId, key)->remove(fiber, tableId, key, snapshot);
    }

    std::shared_ptr<ModificationResponse> revert(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
            const commitmanager::SnapshotDescriptor& snapshot) {
        return shard(tableId, key)->revert(fiber, tableId, key, snapshot);
    }

    std::vector<std::shared_ptr<ScanResponse>> scan(crossbow::infinio::Fiber& fiber, uint64_t tableId,
            const Record& record, ScanMemoryManager& memoryManager, ScanQueryType queryType, uint32_t selectionLength,
            const char* selection, uint32_t queryLength, const char* query,
            const commitmanager::SnapshotDescriptor& snapshot);

    void commit(crossbow::infinio::Fiber& fiber, const commitmanager::SnapshotDescriptor& snapshot);

    std::unique_ptr<crossbow::infinio::InfinibandProcessor> mProcessor;

    commitmanager::ClientSocket mCommitManagerSocket;
    std::vector<std::unique_ptr<store::ClientSocket>> mTellStoreSocket;

    uint64_t mProcessorNum;
    std::atomic<uint64_t> mTransactionCount;
};

/**
 * @brief Class managing all TellStore client processors
 *
 * Dispatches new client functions to the processor with the least amout of load.
 */
class ClientManager : crossbow::non_copyable, crossbow::non_movable {
public:
    ClientManager(const ClientConfig& config);

    void shutdown();

    void execute(std::function<void(ClientHandle&)> fun);

    std::unique_ptr<ScanMemoryManager> allocateScanMemory(size_t chunkCount, size_t chunkLength) {
        return std::unique_ptr<ScanMemoryManager>(new ScanMemoryManager(mService, chunkCount, chunkLength));
    }

private:
    crossbow::infinio::InfinibandService mService;

    std::thread mServiceThread;

    std::vector<std::unique_ptr<ClientProcessor>> mProcessor;
};

} // namespace store
} // namespace tell
