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

#include <config.h>
#include "ScanQuery.hpp"

#include <tellstore/ErrorCode.hpp>
#include <tellstore/Record.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/singleconsumerqueue.hpp>

#include <vector>
#include <unordered_map>
#include <tuple>
#include <memory>
#include <atomic>
#include <thread>
#include <cstring>

namespace tell {
namespace store {

template<class ScanProcessor>
struct ScanThread {
    std::atomic<bool>& stopScans;
    std::atomic<ScanProcessor*> scanProcessor;
    ScanThread(std::atomic<bool>& stopScans)
        : stopScans(stopScans)
        , scanProcessor(nullptr)
    {}
    ScanThread(const ScanThread& o)
        : stopScans(o.stopScans)
        , scanProcessor(nullptr)
    {}
    void operator() () {
        while (!stopScans.load()) {
            if (!scan()) std::this_thread::yield();
        }
    }
    bool scan() {
        auto processor = scanProcessor.load();
        if (processor == nullptr) return false;

        processor->process();

        scanProcessor.store(nullptr);
        return true;
    }
};

template<class Table>
class ScanThreads {
    using ScanRequest = std::tuple<uint64_t, Table*, ScanQuery*>;

    crossbow::SingleConsumerQueue<ScanRequest, MAX_QUERY_SHARING> queryQueue;
    std::vector<ScanRequest> mEnqueuedQueries;
    std::vector<ScanThread<typename Table::ScanProcessor>> threadObjs;
    std::vector<std::thread> threads;
    std::atomic<bool> stopScans;
    std::atomic<bool> stopSlaves;
public:
    ScanThreads(size_t numThreads)
        : mEnqueuedQueries(MAX_QUERY_SHARING, ScanRequest(0u, nullptr, nullptr))
        , threadObjs(numThreads, ScanThread<typename Table::ScanProcessor>(stopSlaves))
        , stopScans(false)
        , stopSlaves(false)
    {}

    ~ScanThreads() {
        stopScans.store(true);
        for (auto& t : threads) t.join();
    }

    void run() {
        for (decltype(threadObjs.size()) i = 0; i < threadObjs.size(); ++i) {
            if (i == 0) {
                // the first thread is the master thread
                threads.emplace_back([this]()
                {
                    while (!stopScans.load()) {
                        if (!masterThread()) std::this_thread::yield();
                    }
                    stopSlaves.store(true);
                });
            } else {
                threads.emplace_back([this, i](){ threadObjs[i](); });
            }
        }
    }

    int scan(uint64_t tableId, Table* table, ScanQuery* query) {
        return (queryQueue.tryWrite(std::make_tuple(tableId, table, query)) ? 0 : error::server_overlad);
    }

private:
    bool masterThread() {
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

            typename Table::Scan scan(table, std::move(queries));
            auto processors = scan.startScan(threadObjs.size());
            for (decltype(threadObjs.size()) i = 0; i < threadObjs.size(); ++i) {
                // we do not need to synchronize here, the scan threads start as soon as the processor is set
                threadObjs[i].scanProcessor.store(processors[i].get());
            }
            // do the master thread part of the scan
            threadObjs[0].scan();

            // now we need to wait until the other threads are done
            for (auto& scan : threadObjs) {
                // as soon as the thread is done, it will unset the processors - this means that the scan is over and
                // the master can delete the processors savely (which will be done as soon as the scope is left).
                while (scan.scanProcessor) std::this_thread::yield();
            }
        }
        return true;
    }
};

} // namespace store
} // namespace tell
