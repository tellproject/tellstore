#pragma once

#include "StorageConfig.hpp"
#include "Epoch.hpp"
#include "Record.hpp"

#include <crossbow/concurrent_map.hpp>
#include <crossbow/string.hpp>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>

namespace tell {
namespace store {
namespace impl {

class NoGC {
public:
    bool doRun() const {
        return false;
    }
    void run() {
        return;
    }
};

template<class Table, class GC>
class TableManager {
private: // Private types
    using Clock = std::chrono::system_clock;
private:
    StorageConfig mConfig;
    GC& mGC;
    std::thread mGCThread;
    bool mShutDown = false;
    crossbow::concurrent_map<crossbow::string, uint64_t> mNames;
    std::vector<Table*> mTables;
    std::atomic<uint64_t> mLastTableIdx;
private:
    void gcThread() {
        auto begin = Clock::now();
        auto duration = std::chrono::seconds(mConfig.gcIntervall);
        while (!mShutDown) {
            auto now = Clock::now();
            if (begin + duration >= now) {
                std::this_thread::sleep_for(begin + duration - now);
            }
            begin = Clock::now();
            mGC.run();
        }
    }
public:
    TableManager(const StorageConfig& config, GC& gc)
            : mConfig(config),
              mGC(gc),
              mGCThread(std::bind(TableManager::mGCThread, this)),
              // TODO: This is a hack, we need to think about a better wat to handle tables (this will evantually crash!!)
              mTables(1024, nullptr),
              mLastTableIdx(0)
    {
    }
    ~TableManager() {
        mShutDown = true;
        mGCThread.join();
    }
public:
    bool createTable(allocator& alloc,
                     const crossbow::string& name,
                     const Schema& schema, uint64_t& idx)
    {
        bool success = false;
        mNames.exec_on(name, [this, &idx](size_t& val){
            if (val != 0) {
                return true;
            }
            val = ++mLastTableIdx;
            idx = val;
            success = true;
            return false;
        });
        if (success) {
            mTables[idx] = new (alloc.malloc(sizeof(Table))) Table(schema);
        }
        return success;
    }

    bool getTableId(const crossbow::string& name, uint64_t& id) {
        auto res = mNames.at(name);
        id = res.second;
        return res.first;
    }
};

} // namespace tell
} // namespace store
} // namespace impl
