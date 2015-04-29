#pragma once

#include "ScanQuery.hpp"
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

struct ScanThread {
    std::atomic<char*> queries;
    std::atomic<bool>& stopScans;
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
        queries.store(nullptr);
        return true;
    }
};

template<class Table>
class ScanThreads {
    int numThreads;
    crossbow::SingleConsumerQueue<ScanRequest<Table>, MAX_QUERY_SHARING> queryQueue;
    std::vector<ScanRequest<Table>> mEnqueuedQueries;
    std::vector<ScanThread> threadObjs;
    std::vector<std::thread> threads;
    std::atomic<bool> stopScans;
public:
    ScanThreads(int numThreads)
        : numThreads(numThreads)
        , mEnqueuedQueries(MAX_QUERY_SHARING
        , ScanRequest<Table>{0, nullptr, nullptr})
        , threadObjs(numThreads, ScanThread(stopScans))
        , stopScans(false)
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
            // TODO: Get the iterators and distribute them to the threads
            for (auto& scan : threadObjs) {
                char* expected = nullptr;
                while (!scan.queries.compare_exchange_strong(expected, queries.get())) std::this_thread::yield();
            }
            // do the master thread part of the scan
            threadObjs[0].scan();
        }
        return true;
    }

    void slaveThread() {
    }
};

} // namespace store
} // namespace tell

