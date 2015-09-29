/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientSocket.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/ScanMemory.hpp>
#include <tellstore/Table.hpp>
#include <tellstore/TransactionType.hpp>

#include <commitmanager/ClientSocket.hpp>
#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/Fiber.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

#include <sparsehash/dense_hash_set>

#include <boost/functional/hash.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <system_error>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

namespace tell {
namespace store {

struct ClientConfig;
class BaseClientProcessor;
class Record;

/**
 * @brief Class representing a TellStore transaction
 */
class ClientTransaction : crossbow::non_copyable {
public:
    ClientTransaction(BaseClientProcessor& processor, crossbow::infinio::Fiber& fiber, TransactionType type,
            bool shared, std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot);

    ~ClientTransaction();

    ClientTransaction(ClientTransaction&& other);

    const commitmanager::SnapshotDescriptor& snapshot() const {
        return *mSnapshot;
    }

    uint64_t version() const {
        return mSnapshot->version();
    }

    TransactionType type() const {
        return mType;
    }

    bool shared() const {
        return mShared;
    }

    bool committed() const {
        return mCommitted;
    }

    std::shared_ptr<GetResponse> get(const Table& table, uint64_t key);

    std::shared_ptr<ModificationResponse> insert(const Table& table, uint64_t key, const GenericTuple& tuple,
            bool hasSucceeded = true);

    std::shared_ptr<ModificationResponse> update(const Table& table, uint64_t key, const GenericTuple& tuple);

    std::shared_ptr<ModificationResponse> remove(const Table& table, uint64_t key);

    std::shared_ptr<ScanIterator> scan(const Table& table, ScanMemoryManager& memoryManager, ScanQueryType queryType,
            uint32_t selectionLength, const char* selection, uint32_t queryLength, const char* query);

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

    void checkTransaction(const Table& table, bool readOnly);

    BaseClientProcessor& mProcessor;
    crossbow::infinio::Fiber& mFiber;

    std::unique_ptr<commitmanager::SnapshotDescriptor> mSnapshot;

    google::dense_hash_set<std::tuple<uint64_t, uint64_t>, ModifiedHasher> mModified;

    TransactionType mType;
    bool mShared;
    bool mCommitted;
};

/**
 * @brief Class to interact with the TellStore from within a fiber
 */
class ClientHandle : crossbow::non_copyable, crossbow::non_movable {
public:
    ClientHandle(BaseClientProcessor& processor, crossbow::infinio::Fiber& fiber);

    crossbow::infinio::Fiber& fiber() {
        return mFiber;
    }

    ClientTransaction startTransaction(TransactionType type = TransactionType::READ_WRITE);

    ClientTransaction startTransaction(TransactionType type,
            std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot);

    Table createTable(const crossbow::string& name, Schema schema);

    std::shared_ptr<GetTableResponse> getTable(const crossbow::string& name);

    std::shared_ptr<GetResponse> get(const Table& table, uint64_t key);

    std::shared_ptr<ModificationResponse> insert(const Table& table, uint64_t key, uint64_t version,
            const GenericTuple& tuple, bool hasSucceeded = true);

    std::shared_ptr<ModificationResponse> update(const Table& table, uint64_t key, uint64_t version,
            const GenericTuple& tuple);

    std::shared_ptr<ModificationResponse> remove(const Table& table, uint64_t key, uint64_t version);

private:
    BaseClientProcessor& mProcessor;
    crossbow::infinio::Fiber& mFiber;
};

/**
 * @brief Class managing all running TellStore fibers
 */
class BaseClientProcessor : crossbow::non_copyable, crossbow::non_movable {
public:
    void shutdown();

protected:
    BaseClientProcessor(crossbow::infinio::InfinibandService& service, const ClientConfig& config,
            uint64_t processorNum);

    ~BaseClientProcessor() = default;

    template <typename Fun>
    void executeFiber(Fun fun) {
        // TODO Starting a fiber without the fiber cache takes ~500us - Investigate why
        mProcessor->executeFiber(std::move(fun));
    }

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

    ClientTransaction start(crossbow::infinio::Fiber& fiber, TransactionType type);

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

    std::shared_ptr<ScanIterator> scan(crossbow::infinio::Fiber& fiber, uint64_t tableId, Record record,
            ScanMemoryManager& memoryManager, ScanQueryType queryType, uint32_t selectionLength, const char* selection,
            uint32_t queryLength, const char* query, const commitmanager::SnapshotDescriptor& snapshot);

    void commit(crossbow::infinio::Fiber& fiber, const commitmanager::SnapshotDescriptor& snapshot);

    std::unique_ptr<crossbow::infinio::InfinibandProcessor> mProcessor;

    commitmanager::ClientSocket mCommitManagerSocket;
    std::vector<std::unique_ptr<store::ClientSocket>> mTellStoreSocket;

    uint64_t mProcessorNum;

    uint16_t mScanId;
};

/**
 * @brief Class managing all running TellStore fibers and its associated context
 */
template <typename Context>
class ClientProcessor : public BaseClientProcessor {
public:
    template <typename... Args>
    ClientProcessor(crossbow::infinio::InfinibandService& service, const ClientConfig& config, uint64_t processorNum,
            Args&&... contextArgs)
            : BaseClientProcessor(service, config, processorNum),
              mTransactionCount(0),
              mContext(std::forward<Args>(contextArgs)...) {
    }

    uint64_t transactionCount() const {
        return mTransactionCount.load();
    }

    template <typename Fun>
    void execute(Fun fun);

private:
    template <typename Fun, typename C = Context>
    typename std::enable_if<std::is_void<C>::value, void>::type executeHandler(Fun& fun, ClientHandle& handle) {
        fun(handle);
    }

    template <typename Fun, typename C = Context>
    typename std::enable_if<!std::is_void<C>::value, void>::type executeHandler(Fun& fun, ClientHandle& handle) {
        fun(handle, mContext);
    }

    std::atomic<uint64_t> mTransactionCount;

    /// The user defined context associated with this processor
    /// In case the context is void we simply allocate a 0-sized array
    typename std::conditional<std::is_void<Context>::value, char[0], Context>::type mContext;
};

template <typename Context>
template <typename Fun>
void ClientProcessor<Context>::execute(Fun fun) {
    ++mTransactionCount;

    executeFiber([this, fun] (crossbow::infinio::Fiber& fiber) mutable {
        ClientHandle handle(*this, fiber);
        executeHandler(fun, handle);

        --mTransactionCount;
    });
}

/**
 * @brief Class managing all TellStore client processors
 *
 * Dispatches new client functions to the processor with the least amout of load.
 */
template <typename Context>
class ClientManager : crossbow::non_copyable, crossbow::non_movable {
public:
    template <typename... Args>
    ClientManager(const ClientConfig& config, Args... contextArgs);

    void shutdown();

    template <typename Fun>
    void execute(Fun fun);

    template <typename Fun>
    void execute(size_t num, Fun fun) {
        mProcessor.at(num)->execute(std::move(fun));
    }

    std::unique_ptr<ScanMemoryManager> allocateScanMemory(size_t chunkCount, size_t chunkLength) {
        return std::unique_ptr<ScanMemoryManager>(new ScanMemoryManager(mService, chunkCount, chunkLength));
    }

private:
    crossbow::infinio::InfinibandService mService;

    std::thread mServiceThread;

    std::vector<std::unique_ptr<ClientProcessor<Context>>> mProcessor;
};

template <typename Context>
template <typename... Args>
ClientManager<Context>::ClientManager(const ClientConfig& config, Args... contextArgs)
        : mService(config.infinibandConfig) {
    LOG_INFO("Starting client manager");

    // TODO Move the service thread into the Infiniband Service itself
    mServiceThread = std::thread([this] () {
        mService.run();
    });

    mProcessor.reserve(config.numNetworkThreads);
    for (decltype(config.numNetworkThreads) i = 0; i < config.numNetworkThreads; ++i) {
        mProcessor.emplace_back(new ClientProcessor<Context>(mService, config, i, contextArgs...));
    }
}

template <typename Context>
void ClientManager<Context>::shutdown() {
    LOG_INFO("Shutting down client manager");
    for (auto& proc : mProcessor) {
        proc->shutdown();
    }

    LOG_INFO("Waiting for transactions to terminate");
    for (auto& proc : mProcessor) {
        while (proc->transactionCount() != 0) {
            std::this_thread::yield();
        }
    }
}

template <typename Context>
template <typename Fun>
void ClientManager<Context>::execute(Fun fun) {
    ClientProcessor<Context>* processor = nullptr;
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

    processor->execute(std::move(fun));
}

} // namespace store
} // namespace tell
