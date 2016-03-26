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

#include "Allocator.hpp"
#include "StorageConfig.hpp"
#include "Scan.hpp"
#include "VersionManager.hpp"

#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

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

template<class Storage>
class TableManager {
private: // Private types
    using Table = typename Storage::Table;
    using GC = typename Storage::GC;
    using Clock = std::chrono::system_clock;
private:
    MemoryReclaimer& mMemoryManager;
    PageManager& mPageManager;
    VersionManager& mVersionManager;
    GC& mGC;
    StorageConfig mConfig;
    ScanManager<Storage> mScanManager;
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
        auto duration = std::chrono::seconds(mConfig.gcInterval);
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
    TableManager(Storage& storage, MemoryReclaimer& memoryManager, PageManager& pageManager,
        VersionManager& versionManager, GC& gc, const StorageConfig& config)
        : mMemoryManager(memoryManager)
        , mPageManager(pageManager)
        , mVersionManager(versionManager)
        , mGC(gc)
        , mConfig(config)
        , mScanManager(storage, config.numScanThreads)
        , mShutDown(false)
        , mLastTableIdx(0)
        , mGCThread(std::bind(&TableManager::gcThread, this))
    {
    }

    ~TableManager() {
        mShutDown.store(true);
        mStopCondition.notify_all();
        mGCThread.join();
        for (auto t : mTables) {
            delete t.second;
        }
    }

public:
    const StorageConfig& config() const {
        return mConfig;
    }

    template <typename... Args>
    bool createTable(const crossbow::string& name,
                     const Schema& schema,
                     uint64_t& idx,
                     Args&&... args) {
        if (schema.type() == TableType::UNKNOWN) {
            return false;
        }

        typename decltype(mTablesMutex)::scoped_lock _(mTablesMutex, false);
        idx = ++mLastTableIdx;
        {
            auto res = mNames.insert(std::make_pair(name, idx));
            if (!res.second) {
                return false;
            }
        }

        auto ptr = new Table(mMemoryManager, mPageManager, name, schema, idx, std::forward<Args>(args)...);
        LOG_ASSERT(ptr, "Unable to allocate table");
        __attribute__((unused)) auto res = mTables.insert(std::make_pair(idx, ptr));
        LOG_ASSERT(res.second, "Insert with unique id failed");

        return true;
    }

    std::vector<const Table*> getTables() const {
        typename decltype(mTablesMutex)::scoped_lock _(mTablesMutex, false);
        std::vector<const Table*> result;
        result.reserve(mTables.size());

        for (auto& e : mTables) {
            result.emplace_back(e.second);
        }
        return result;
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

    template <typename Fun>
    int get(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, Fun fun)
    {
        mVersionManager.addSnapshot(snapshot);
        return executeTable(tableId, [key, &snapshot, &fun] (Table* table) {
            return table->get(key, snapshot, fun);
        });
    }

    int update(uint64_t tableId, uint64_t key, size_t size, const char* data,
            const commitmanager::SnapshotDescriptor& snapshot)
    {
        mVersionManager.addSnapshot(snapshot);
        return executeTable(tableId, [key, size, data, &snapshot] (Table* table) {
            return table->update(key, size, data, snapshot);
        });
    }

    int insert(uint64_t tableId, uint64_t key, size_t size, const char* data,
            const commitmanager::SnapshotDescriptor& snapshot)
    {
        mVersionManager.addSnapshot(snapshot);
        return executeTable(tableId, [key, size, data, &snapshot] (Table* table) {
            return table->insert(key, size, data, snapshot);
        });
    }

    int remove(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot)
    {
        mVersionManager.addSnapshot(snapshot);
        return executeTable(tableId, [key, &snapshot] (Table* table) {
            return table->remove(key, snapshot);
        });
    }

    int revert(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot)
    {
        mVersionManager.addSnapshot(snapshot);
        return executeTable(tableId, [key, &snapshot] (Table* table) {
            return table->revert(key, snapshot);
        });
    }

    int scan(uint64_t tableId, ScanQuery* query) {
        if (query && query->snapshot()) {
            mVersionManager.addSnapshot(*query->snapshot());
        }
        return executeTable(tableId, [this, tableId, query] (Table* table) {
            return mScanManager.scan(tableId, table, query);
        });
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

    template <typename Fun>
    int executeTable(uint64_t tableId, Fun fun) {
        auto table = lookupTable(tableId);
        if (!table) {
            return error::invalid_table;
        }
        return fun(table);
    }
};

} // namespace store
} // namespace tell
