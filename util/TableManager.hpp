#pragma once

#include "StorageConfig.hpp"
#include "Record.hpp"
#include "Scan.hpp"

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
    std::atomic<uint64_t> mLowestActiveVersion;
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
        auto oldLowestActiveVersion = 0x1u;
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
            mGC.run(tables, mLowestActiveVersion.load());
        }
    }

public:
    TableManager(PageManager& pageManager, const StorageConfig& config, GC& gc)
        : mConfig(config)
        , mGC(gc)
        , mPageManager(pageManager)
        , mLowestActiveVersion(0x1u)
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
        updateLowestActiveVersion(snapshot);
        return lookupTable(tableId)->get(key, size, data, snapshot, version, isNewest);
    }

    bool update(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        crossbow::allocator _;
        updateLowestActiveVersion(snapshot);
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
        updateLowestActiveVersion(snapshot);
        lookupTable(tableId)->insert(key, size, data, snapshot, succeeded);
    }

    bool remove(uint64_t tableId,
                uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        crossbow::allocator _;
        updateLowestActiveVersion(snapshot);
        return lookupTable(tableId)->remove(key, snapshot);
    }

    bool revert(uint64_t tableId,
                uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        crossbow::allocator _;
        updateLowestActiveVersion(snapshot);
        return lookupTable(tableId)->revert(key, snapshot);
    }

    int numScanThreads() const {
        return mScanThreads.numThreads();
    }

    bool scan(uint64_t tableId, char* query, size_t querySize, const std::vector<ScanQueryImpl*>& impls) {
        ScanRequest<Table> request;
        request.tableId = tableId;
        request.table = lookupTable(tableId);
        request.query = query;
        request.querySize = querySize;
        request.impls = impls;
        return mScanThreads.scan(std::move(request));
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

    void updateLowestActiveVersion(const commitmanager::SnapshotDescriptor& snapshot) {
        auto lowestActiveVersion = mLowestActiveVersion.load();
        while (lowestActiveVersion < snapshot.lowestActiveVersion()) {
            if (!mLowestActiveVersion.compare_exchange_strong(lowestActiveVersion, snapshot.lowestActiveVersion())) {
                continue;
            }
            return;
        }
    }
};

} // namespace store
} // namespace tell
