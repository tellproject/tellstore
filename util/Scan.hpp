#pragma once

#include "IteratorEntry.hpp"
#include "ScanQuery.hpp"
#include "Record.hpp"
#include <config.h>

#include <crossbow/singleconsumerqueue.hpp>
#include <vector>
#include <unordered_map>
#include <tuple>
#include <memory>
#include <atomic>
#include <thread>
#include <memory.h>

namespace tell {
namespace store {

template<class Table>
struct ScanRequest {
    uint64_t tableId;
    Table* table;
    char* query;
    size_t querySize;
};

template<class Iterator>
struct ScanThread {
    std::atomic<char*> queries;
    std::atomic<bool>& stopScans;
    std::atomic<Iterator*> beginIter;
    std::atomic<Iterator*> endIter;
    ScanThread(std::atomic<bool>& stopScans)
        : queries(nullptr)
        , stopScans(stopScans)
    {}
    ScanThread(const ScanThread& o)
        : queries(nullptr)
        , stopScans(o.stopScans)
    {}
    void operator() () {
        while (!stopScans.load()) {
            if (!scan()) std::this_thread::yield();
        }
    }
    bool scan() {
        auto qbuffer = queries.load();
        if (qbuffer == nullptr) return false;
        auto iter = *beginIter;
        auto end = *endIter;
        const Record& record = *iter->record;
        ScanQuery query;
        std::vector<bool> queryBitMap;
        uint64_t numQueries = *reinterpret_cast<uint64_t*>(qbuffer);
        qbuffer += 8;
        for (; iter != end; ++iter) {
            const auto& entry = *iter;
            for (uint64_t i = 0; i < numQueries; ++i) {
                query.query = qbuffer;
                qbuffer = query.check(entry.data, queryBitMap, record);
                queryBitMap.clear();
            }
        }
        beginIter.store(nullptr);
        endIter.store(nullptr);
        queries.store(nullptr);
        return true;
    }
};

template<class Table>
class ScanThreads {
    int numThreads;
    crossbow::SingleConsumerQueue<ScanRequest<Table>, MAX_QUERY_SHARING> queryQueue;
    std::vector<ScanRequest<Table>> mEnqueuedQueries;
    std::vector<ScanThread<typename Table::Iterator>> threadObjs;
    std::vector<std::thread> threads;
    std::atomic<bool> stopScans;
    std::atomic<bool> stopSlaves;
public:
    ScanThreads(int numThreads)
        : numThreads(numThreads)
        , mEnqueuedQueries(MAX_QUERY_SHARING
        , ScanRequest<Table>{0, nullptr, nullptr})
        , threadObjs(numThreads, ScanThread<typename Table::Iterator>(stopSlaves))
        , stopScans(false)
        , stopSlaves(false)
    {}
    ~ScanThreads() {
        stopScans.store(true);
        for (auto& t : threads) t.join();
    }
    void run() {
        for (int i = 0; i < numThreads; ++i) {
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
private:
    bool masterThread() {
        // A map of all queries we get during this scan phase. Key is the table id, value is:
        //  - the Table object
        //  - the total size
        //  - a vector of queries - that means the query object and the size of the query
        std::unordered_map<uint64_t, std::tuple<Table*, size_t, std::vector<std::pair<char*, size_t>>>> queryMap;
        auto numQueries = queryQueue.readMultiple(mEnqueuedQueries.begin(), mEnqueuedQueries.end());
        if (numQueries == 0) return false;
        for (auto q : mEnqueuedQueries) {
            auto iter = queryMap.find(q.tableId);
            if (iter == queryMap.end()) {
                auto res = queryMap.emplace(q.tableId, std::make_tuple(q.table, 0, std::vector<std::pair<char*, size_t>>()));
                iter = res.first;
            }
            std::get<1>(iter->second) += q.querySize;
            std::get<2>(iter->second).emplace_back(q.query, q.querySize);
        }
        // now we have all queries in a map, so we can start the scans
        for (auto& q : queryMap) {
            // first we need to create the QBuffer
            // The QBuffer is the shared object of all scans, it is a byte array, where
            // the first 8 bytes encode the number of queries followed by the serialized
            // queries themselves.
            std::unique_ptr<char[]> queries(new char[std::get<1>(q.second) + 8]);
            *reinterpret_cast<uint64_t*>(queries.get()) = std::get<2>(q.second).size();
            size_t offset = 8;
            for (auto& p : std::get<2>(q.second)) {
                memcpy(queries.get() + offset, p.first, p.second);
                offset += p.second;
            }
            // now we generated the QBuffer - we now give it to all the scan threads
            auto iterators = std::get<0>(q.second)->startScan(numThreads);
            for (size_t i = 0; i < threadObjs.size(); ++i) {
                auto& scan = threadObjs[i];
                // we do not need to synchronize here, we do that as soon as
                // the scan is over. We use atomics here because the order of
                // assignment is crucial: the master will set the queries last
                // and the slaves will unset the queries last - therefore the
                // queries atomic is our point of synchronisation
                scan.beginIter.store(&iterators[i].first);
                scan.endIter.store(&iterators[i].second);
                scan.queries.store(queries.get());
            }
            // do the master thread part of the scan
            threadObjs[0].scan();
            // now we need to wait until the other threads are done
            for (auto& scan : threadObjs) {
                // as soon as the thread is done, it will unset the queries
                // and iterators - it is important that the queries are
                // unset last, this means that the scan is over and the
                // master can delete the iterators savely (which will be
                // done as soon as the scope is left).
                while (scan.queries) std::this_thread::yield();
            }
        }
        return true;
    }

    void slaveThread() {
    }
};

} // namespace store
} // namespace tell

