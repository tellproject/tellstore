#pragma once

#include "StorageConfig.hpp"
#include "Scan.hpp"
#include "VersionManager.hpp"

#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/concurrent_map.hpp>
#include <crossbow/string.hpp>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <atomic>

#include <tbb/spin_rw_mutex.h>

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

class ScanQuery;
class PageManager;

class NoGC {
public:
};

template<class Table, class GC>
class TableManager {
private: // Private types
    using Clock = std::chrono::system_clock;
private:
    StorageConfig mConfig;
    GC& mGC;
    PageManager& mPageManager;
    VersionManager& mVersionManager;
    ScanThreads<Table> mScanThreads;
    std::atomic<bool> mShutDown;
    mutable tbb::spin_rw_mutex mTablesMutex;
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
                typename decltype(mTablesMutex)::scoped_lock _(mTablesMutex, false);
                for (auto& p : mTables) {
                    tables.push_back(p.second);
                }
            }
            mGC.run(tables, mVersionManager.lowestActiveVersion());
        }
    }

public:
    TableManager(PageManager& pageManager, const StorageConfig& config, GC& gc, VersionManager& versionManager)
        : mConfig(config)
        , mGC(gc)
        , mPageManager(pageManager)
        , mVersionManager(versionManager)
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
            crossbow::allocator::destroy_now(t.second);
        }
    }

public:
    template <typename... Args>
    bool createTable(const crossbow::string& name,
                     const Schema& schema,
                     uint64_t& idx,
                     Args&&... args) {
        if (schema.type() == TableType::UNKNOWN) {
            return false;
        }

        crossbow::allocator __;
        typename decltype(mTablesMutex)::scoped_lock _(mTablesMutex, false);
        idx = ++mLastTableIdx;
        auto res = mNames.insert(std::make_pair(name, idx));
        if (!res.second) {
            return false;
        }

        auto ptr = crossbow::allocator::construct<Table>(mPageManager, schema, idx, std::forward<Args>(args)...);
        LOG_ASSERT(ptr, "Unable to allocate table");
        mTables[idx] = ptr;
        return true;
    }

    const Table* getTable(uint64_t id) const {
        return lookupTable(id);
    }

    const Table* getTable(const crossbow::string& name, uint64_t& id) const {
        typename decltype(mTablesMutex)::scoped_lock _(mTablesMutex, false);
        auto res = mNames.find(name);
        if (res == mNames.end()) return nullptr;
        id = res->second;
        return lookupTable(res->second);
    }

    bool get(uint64_t tableId,
             uint64_t key,
             size_t& size,
             const char*& data,
             const commitmanager::SnapshotDescriptor& snapshot,
             uint64_t& version,
             bool& isNewest)
    {
        crossbow::allocator _;
        mVersionManager.addSnapshot(snapshot);
        return lookupTable(tableId)->get(key, size, data, snapshot, version, isNewest);
    }

    bool update(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        crossbow::allocator _;
        mVersionManager.addSnapshot(snapshot);
        return lookupTable(tableId)->update(key, size, data, snapshot);
    }


    void insert(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const commitmanager::SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr)
    {
        crossbow::allocator _;
        mVersionManager.addSnapshot(snapshot);
        lookupTable(tableId)->insert(key, size, data, snapshot, succeeded);
    }

    bool remove(uint64_t tableId,
                uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        crossbow::allocator _;
        mVersionManager.addSnapshot(snapshot);
        return lookupTable(tableId)->remove(key, snapshot);
    }

    bool revert(uint64_t tableId,
                uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        crossbow::allocator _;
        mVersionManager.addSnapshot(snapshot);
        return lookupTable(tableId)->revert(key, snapshot);
    }

    bool scan(uint64_t tableId, ScanQuery* query) {
        mVersionManager.addSnapshot(query->snapshot());
        return mScanThreads.scan(tableId, lookupTable(tableId), query);
    }

    void forceGC() {
        // Notifies the GC
        mStopCondition.notify_all();
    }

private:
    const Table* lookupTable(uint64_t tableId) const {
        typename decltype(mTablesMutex)::scoped_lock _(mTablesMutex, false);
        auto i = mTables.find(tableId);
        return (i == mTables.end() ? nullptr : i->second);
    }

    Table* lookupTable(uint64_t tableId) {
        return const_cast<Table*>(const_cast<const TableManager*>(this)->lookupTable(tableId));
    }
};

} // namespace store
} // namespace tell
