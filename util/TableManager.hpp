#pragma once

#include "StorageConfig.hpp"
#include "Epoch.hpp"
#include "Record.hpp"
#include "CommitManager.hpp"

#include <crossbow/concurrent_map.hpp>
#include <crossbow/string.hpp>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <atomic>

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
    std::thread mGCThread;
    CommitManager mCommitManager;
    bool mShutDown = false;
    crossbow::concurrent_map<crossbow::string, uint64_t> mNames;
    std::vector<Table*> mTables;
    std::atomic<uint64_t> mLastTableIdx;
    std::condition_variable mStopCondition;
private:
    void gcThread() {
        std::mutex m;
        std::unique_lock<std::mutex> lock(m);
        auto begin = Clock::now();
        auto duration = std::chrono::seconds(mConfig.gcIntervall);
        while (!mShutDown) {
            auto now = Clock::now();
            if (begin + duration >= now) {
                mStopCondition.wait_for(lock, begin + duration - now);
            }
            if (mShutDown) return;
            begin = Clock::now();
            mGC.run(mTables, mCommitManager.getLowestActiveVersion());
        }
    }

public:
    TableManager(PageManager& pageManager, const StorageConfig& config, GC& gc)
        : mConfig(config), mGC(gc), mGCThread(std::bind(&TableManager::gcThread, this)),
          mPageManager(pageManager),
          // TODO: This is a hack, we need to think about a better wat to handle tables (this will eventually crash!!)
          mTables(1024, nullptr), mLastTableIdx(0) {
    }

    ~TableManager() {
        mShutDown = true;
        mStopCondition.notify_all();
        mGCThread.join();
    }

public:
    bool createTable(const crossbow::string& name,
                     const Schema& schema,
                     uint64_t& idx) {
        bool success = false;
        mNames.exec_on(name, [this, &idx, &success](uint64_t& val) {
            if (val != 0) {
                return true;
            }
            val = ++mLastTableIdx;
            idx = val;
            success = true;
            return false;
        });
        if (success) {
            mTables[idx] = new(tell::store::malloc(sizeof(Table))) Table(mPageManager, schema);
        }
        return success;
    }

    bool getTableId(const crossbow::string& name, uint64_t& id) {
        auto res = mNames.at(name);
        id = res.second;
        return res.first;
    }

    bool get(uint64_t tableId,
             uint64_t key,
             const char*& data,
             const SnapshotDescriptor& snapshot,
             bool& isNewest)
    {
        return mTables[tableId]->get(key, data, snapshot, isNewest);
    }

    bool update(uint64_t tableId,
                uint64_t key,
                const char* const data,
                const SnapshotDescriptor& snapshot)
    {
        return mTables[tableId]->update(key, data, snapshot);
    }


    void insert(uint64_t tableId,
                uint64_t key,
                const char* const data,
                const SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr)
    {
        mTables[tableId]->insert(key, data, snapshot, succeeded);
    }

    bool remove(uint64_t tableId,
                uint64_t key,
                const SnapshotDescriptor& snapshot)
    {
        return mTables[tableId]->remove(key, snapshot);
    }

    void forceGC() {
        // Notifies the GC
        mStopCondition.notify_all();
    }
};

} // namespace store
} // namespace tell
