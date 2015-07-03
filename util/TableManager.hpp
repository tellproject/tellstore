#pragma once

#include "StorageConfig.hpp"
#include "Epoch.hpp"
#include "Record.hpp"
#include "CommitManager.hpp"
#include "Scan.hpp"

#include <crossbow/concurrent_map.hpp>
#include <crossbow/string.hpp>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <atomic>

#include <tbb/queuing_rw_mutex.h>

namespace tbb {
inline size_t tbb_hasher(const crossbow::string& str)
{
    std::hash<crossbow::string> h;
    return h(str);
}
} // namespace tbb

#include <tbb/concurrent_unordered_map.h>

namespace tell {
namespace store {

class NoGC {
public:
};

class PageManager;

template<class Table, class GC>
class TableManager {
private: // Private types
    using Clock = std::chrono::system_clock;
private:
    StorageConfig mConfig;
    GC& mGC;
    PageManager& mPageManager;
    CommitManager& mCommitManager;
    ScanThreads<Table> mScanThreads;
    std::atomic<bool> mShutDown;
    mutable tbb::queuing_rw_mutex mTablesMutex;
    tbb::concurrent_unordered_map<crossbow::string, uint64_t> mNames;
    tbb::concurrent_unordered_map<uint64_t, Table*> mTables;
    std::atomic<uint64_t> mLastTableIdx;
    std::condition_variable mStopCondition;
    mutable std::mutex mGCMutex;
    std::thread mGCThread;
private:
    void gcThread() {
        std::unique_lock<std::mutex> lock(mGCMutex);
        auto begin = Clock::now();
        auto duration = std::chrono::seconds(mConfig.gcIntervall);
        while (!mShutDown.load()) {
            auto now = Clock::now();
            if (begin + duration > now) {
                mStopCondition.wait_until(lock, begin + duration);
            }
            if (mShutDown.load()) return;
            begin = Clock::now();
            std::vector<Table*> tables;
            tables.reserve(mNames.size());
            {
                tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
                for (auto& p : mTables) {
                    tables.push_back(p.second);
                }
            }
            mGC.run(tables, mCommitManager.getLowestActiveVersion());
        }
    }

public:
    TableManager(PageManager& pageManager, const StorageConfig& config, GC& gc, CommitManager& commitManager)
        : mConfig(config)
        , mGC(gc)
        , mPageManager(pageManager)
        , mCommitManager(commitManager)
        , mScanThreads(config.numScanThreads)
        , mShutDown(false)
        , mLastTableIdx(0)
        , mGCThread(std::bind(&TableManager::gcThread, this))
    {
        mScanThreads.run();
    }

    ~TableManager() {
        mShutDown.store(true);
        mStopCondition.notify_all();
        mGCThread.join();
        for (auto t : mTables) {
            allocator::destroy_now(t.second);
        }
    }

public:
    template <typename... Args>
    bool createTable(const crossbow::string& name,
                     const Schema& schema,
                     uint64_t& idx,
                     Args&&... args) {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        idx = ++mLastTableIdx;
        auto res = mNames.insert(std::make_pair(name, idx));
        if (!res.second) {
            return false;
        }

        auto ptr = allocator::construct<Table>(mPageManager, schema, idx, std::forward<Args>(args)...);
        LOG_ASSERT(ptr, "Unable to allocate table");
        mTables[idx] = ptr;
        return true;
    }

    bool getTableId(const crossbow::string& name, uint64_t& id) {
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        auto res = mNames.find(name);
        if (res == mNames.end()) return false;
        id = res->second;
        return true;
    }

    bool get(uint64_t tableId,
             uint64_t key,
             size_t& size,
             const char*& data,
             const SnapshotDescriptor& snapshot,
             bool& isNewest)
    {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        return mTables[tableId]->get(key, size, data, snapshot, isNewest);
    }

    bool getNewest(uint64_t tableId,
                   uint64_t key,
                   size_t& size,
                   const char*& data,
                   uint64_t& version)
    {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        return mTables[tableId]->getNewest(key, size, data, version);
    }

    bool update(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const SnapshotDescriptor& snapshot)
    {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        return mTables[tableId]->update(key, size, data, snapshot);
    }


    void insert(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr)
    {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        mTables[tableId]->insert(key, size, data, snapshot, succeeded);
    }

    void insert(uint64_t tableId,
                uint64_t key,
                const GenericTuple& tuple,
                const SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr)
    {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        mTables[tableId]->insert(key, tuple, snapshot, succeeded);
    }

    bool remove(uint64_t tableId,
                uint64_t key,
                const SnapshotDescriptor& snapshot)
    {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        return mTables[tableId]->remove(key, snapshot);
    }

    bool revert(uint64_t tableId,
                uint64_t key,
                const SnapshotDescriptor& snapshot)
    {
        allocator __;
        tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
        return mTables[tableId]->revert(key, snapshot);
    }

    int numScanThreads() const {
        return mScanThreads.numThreads();
    }

    bool scan(uint64_t tableId, char* query, size_t querySize, const std::vector<ScanQueryImpl*>& impls) {
        ScanRequest<Table> request;
        request.tableId = tableId;
        {
            tbb::queuing_rw_mutex::scoped_lock _(mTablesMutex, false);
            request.table = mTables[tableId];
        }
        request.query = query;
        request.querySize = querySize;
        request.impls = impls;
        return mScanThreads.scan(std::move(request));
    }

    void forceGC() {
        // Notifies the GC
        mStopCondition.notify_all();
    }
};

} // namespace store
} // namespace tell
