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
#include "ScanQuery.hpp"

#include <config.h>
#include <tellstore/ErrorCode.hpp>
#include <tellstore/Record.hpp>

#include <crossbow/non_copyable.hpp>
#include <crossbow/singleconsumerqueue.hpp>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <tuple>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tell {
namespace store {

template<class Table>
class ScanThread : crossbow::non_copyable, crossbow::non_movable {
public:
    ScanThread()
            : mData(0),
              mThread(&ScanThread<Table>::operator(), this) {
    }

    void stop();

    void prepare(typename Table::Scan* scan) {
        notify(scan, PointerTag::PREPARE);
    }

    void process(typename Table::ScanProcessor* processor) {
        notify(processor, PointerTag::PROCESS);
    }

    bool isBusy() const {
        return (mData.load() != 0);
    }

private:
    enum class PointerTag : uintptr_t {
        STOP = 0x1u,
        PREPARE = (0x1u << 1),
        PROCESS = (0x1u << 2),
    };

    void operator()();

    void notify(void* data, PointerTag tag);

    std::atomic<uintptr_t> mData;

    std::mutex mWaitMutex;
    std::condition_variable mWaitCondition;

    std::thread mThread;
};

template <class Table>
void ScanThread<Table>::stop() {
    notify(nullptr, PointerTag::STOP);

    mThread.join();
}

template <class Table>
void ScanThread<Table>::operator()() {
    while (true) {
        uintptr_t data = 0;
        {
            std::unique_lock<decltype(mWaitMutex)> waitLock(mWaitMutex);
            mWaitCondition.wait(waitLock, [this, &data] () {
                data = mData.load();
                return data != 0;
            });
        }

        if ((data & crossbow::to_underlying(PointerTag::STOP)) != 0) {
            break;
        }

        if ((data & crossbow::to_underlying(PointerTag::PREPARE)) != 0) {
            auto scan = reinterpret_cast<typename Table::Scan*>(data & ~crossbow::to_underlying(PointerTag::PREPARE));
            scan->prepareMaterialization();
        } else if ((data & crossbow::to_underlying(PointerTag::PROCESS)) != 0) {
            auto processor = reinterpret_cast<typename Table::ScanProcessor*>(data &
                    ~crossbow::to_underlying(PointerTag::PROCESS));
            processor->process();
        } else {
            LOG_ASSERT(false, "Unknown pointer tag");
        }

        mData.store(0);
    }
}

template <class Table>
void ScanThread<Table>::notify(void* data, ScanThread::PointerTag tag) {
    {
        std::unique_lock<decltype(mWaitMutex)> waitLock(mWaitMutex);
        mData.store(reinterpret_cast<uintptr_t>(data) | crossbow::to_underlying(tag));
    }
    mWaitCondition.notify_one();
}

template<class Storage>
class ScanManager : crossbow::non_copyable, crossbow::non_movable {
    using Table = typename Storage::Table;
    using ScanRequest = std::tuple<uint64_t, Table*, ScanQuery*>;

    size_t mNumThreads;

    std::unique_ptr<MemoryConsumer> mMemoryConsumer;

    crossbow::SingleConsumerQueue<ScanRequest, MAX_QUERY_SHARING> queryQueue;
    std::vector<ScanRequest> mEnqueuedQueries;
    std::atomic<bool> stopScans;

    std::vector<std::unique_ptr<ScanThread<Table>>> mSlaves;
    std::thread mMasterThread;
public:
    ScanManager(Storage& storage, size_t numThreads)
        : mNumThreads(numThreads)
        , mMemoryConsumer(storage.createMemoryConsumer())
        , mEnqueuedQueries(MAX_QUERY_SHARING, ScanRequest(0u, nullptr, nullptr))
        , stopScans(false) {
        if (mNumThreads == 0) {
            LOG_WARN("No scan threads set - Scan will be unavailable");
            return;
        }

        mSlaves.reserve(mNumThreads - 1);
        for (decltype(mNumThreads) i = 0; i < mNumThreads - 1; ++i) {
            mSlaves.emplace_back(new ScanThread<Table>());
        }

        mMasterThread = std::thread(&ScanManager<Storage>::operator(), this);
    }

    ~ScanManager() {
        stopScans.store(true);
        if (mNumThreads != 0u) {
            mMasterThread.join();
        }
    }

    int scan(uint64_t tableId, Table* table, ScanQuery* query) {
        return (queryQueue.tryWrite(std::make_tuple(tableId, table, query)) ? 0 : error::server_overlad);
    }

private:
    void operator()();

    bool masterThread();
};

template<class Storage>
void ScanManager<Storage>::operator()() {
    while (!stopScans.load()) {
        if (!masterThread()) {
            std::this_thread::yield();
        }
    }
    for (auto& slave : mSlaves) {
        slave->stop();
    }
}

template<class Storage>
bool ScanManager<Storage>::masterThread() {
    // A map of all queries we get during this scan phase. Key is the table id, value is:
    //  - the Table object
    //  - the total size
    //  - a vector of queries - that means the query object and the size of the query
    std::unordered_map<uint64_t, std::tuple<Table*, std::vector<ScanQuery*>>> queryMap;
    auto numQueries = queryQueue.readMultiple(mEnqueuedQueries.begin(), mEnqueuedQueries.end());
    if (numQueries == 0) return false;

    for (size_t i = 0; i < numQueries; ++i) {
        uint64_t tableId;
        Table* table;
        ScanQuery* query;
        std::tie(tableId, table, query) = mEnqueuedQueries.at(i);
        auto iter = queryMap.find(tableId);
        if (iter == queryMap.end()) {
            auto res = queryMap.emplace(tableId, std::make_tuple(table, std::vector<ScanQuery*>()));
            iter = res.first;
        }
        if (!query) {
            continue;
        }
        std::get<1>(iter->second).emplace_back(query);
    }
    // now we have all queries in a map, so we can start the scans
    for (auto& q : queryMap) {
        // first we need to create the QBuffer
        // The QBuffer is the shared object of all scans, it is a byte array containing the combined serialized
        // selection queries of every scan query.
        Table* table;
        std::vector<ScanQuery*> queries;
        std::tie(table, queries) = std::move(q.second);

        //auto startTime = std::chrono::steady_clock::now();
        //auto queryCount = queries.size();
        typename Table::Scan scan(table, std::move(queries));

        if (!mSlaves.empty()) {
            mSlaves.front()->prepare(&scan);
        } else {
            scan.prepareMaterialization();
        }
        scan.prepareQuery();
        if (!mSlaves.empty()) {
            auto& slave = mSlaves.front();
            while (slave->isBusy()) std::this_thread::yield();
        }
        //auto prepareTime = std::chrono::steady_clock::now();

        MemoryConsumerLock memoryLock(mMemoryConsumer.get());

        auto processors = scan.startScan(mNumThreads);
        for (decltype(mSlaves.size()) i = 0; i < mSlaves.size(); ++i) {
            // we do not need to synchronize here, the scan threads start as soon as the processor is set
            mSlaves[i]->process(processors[i].get());
        }

        // do the master thread part of the scan
        processors[mSlaves.size()]->process();

        // now we need to wait until the other threads are done
        for (auto& slave : mSlaves) {
            // as soon as the thread is done, it will unset the processors - this means that the scan is over and
            // the master can delete the processors savely (which will be done as soon as the scope is left).
            while (slave->isBusy()) std::this_thread::yield();
        }
        //auto endTime = std::chrono::steady_clock::now();

        memoryLock.release();

        //auto prepareDuration = std::chrono::duration_cast<std::chrono::milliseconds>(prepareTime - startTime);
        //auto processDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - prepareTime);
        //auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        //LOG_INFO("Scan took %1%ms for %2% queries [prepare = %3%, process = %4%]",
        //         totalDuration.count(), queryCount, prepareDuration.count(), processDuration.count());
    }
    return true;
}

} // namespace store
} // namespace tell
