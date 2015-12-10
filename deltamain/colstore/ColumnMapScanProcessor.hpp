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

#include <util/LLVMScan.hpp>
#include <util/Log.hpp>

#include <crossbow/allocator.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {

class Record;

namespace deltamain {

class ColumnMapContext;
struct ColumnMapMainEntry;
struct ColumnMapMainPage;
struct InsertLogEntry;
struct UpdateLogEntry;

template <typename Context>
class Table;

class ColumnMapScanProcessor;

class ColumnMapScan : public LLVMRowScanBase {
public:
    using ScanProcessor = ColumnMapScanProcessor;

    using ColumnScanFun = void (*) (const uint64_t* /* keyData */, const uint64_t* /* validFromData */,
            const uint64_t* /* validToData */, const char* /* page */, uint64_t /* startIdx */, uint64_t /* endIdx */,
            char* /* resultData */);

    using ColumnProjectionFun = uint32_t (*) (const char* /* page */, uint64_t /* idx */, char* /* destData */);

    using ColumnAggregationFun = uint32_t (*) (const char* /* page */, uint64_t /* startIdx */, uint64_t /* endIdx */,
            const char* /* resultData */, char* /* destData */);

    ColumnMapScan(Table<ColumnMapContext>* table, std::vector<ScanQuery*> queries);

    std::vector<std::unique_ptr<ColumnMapScanProcessor>> startScan(size_t numThreads);

private:
    void prepareColumnScanFunction(const Record& record);

    void prepareColumnProjectionFunction(const ColumnMapContext& context, ScanQuery* query, uint32_t index);

    void prepareColumnAggregationFunction(const ColumnMapContext& context, ScanQuery* query, uint32_t index);

    Table<ColumnMapContext>* mTable;

    ColumnScanFun mColumnScanFun;

    std::vector<ColumnProjectionFun> mColumnProjectionFuns;

    std::vector<ColumnAggregationFun> mColumnAggregationFuns;

    crossbow::allocator mAllocator;
};

class ColumnMapScanProcessor : public LLVMRowScanProcessorBase {
public:
    using LogIterator = Log<OrderedLogImpl>::ConstLogIterator;
    using PageList = std::vector<ColumnMapMainPage*>;

    ColumnMapScanProcessor(const ColumnMapContext& context, const Record& record,
            const std::vector<ScanQuery*>& queries, const PageList& pages, size_t pageIdx, size_t pageEndIdx,
            const LogIterator& logIter, const LogIterator& logEnd, ColumnMapScan::ColumnScanFun columnScanFun,
            const std::vector<ColumnMapScan::ColumnProjectionFun>& columnProjectionFuns,
            const std::vector<ColumnMapScan::ColumnAggregationFun>& columnAggregationFuns,
            ColumnMapScan::RowScanFun rowScanFun,
            const std::vector<ColumnMapScan::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts);

    void process();

private:
    void processMainPage(const ColumnMapMainPage* page, uint64_t startIdx, uint64_t endIdx);

    void evaluateMainQueries(const ColumnMapMainPage* page, uint64_t startIdx, uint64_t endIdx);

    uint64_t processUpdateRecord(const UpdateLogEntry* ptr, uint64_t baseVersion, uint64_t& validTo);

    const ColumnMapContext& mContext;

    ColumnMapScan::ColumnScanFun mColumnScanFun;

    std::vector<ColumnMapScan::ColumnProjectionFun> mColumnProjectionFuns;

    std::vector<ColumnMapScan::ColumnAggregationFun> mColumnAggregationFuns;

    const PageList& pages;
    size_t pageIdx;
    size_t pageEndIdx;
    LogIterator logIter;
    LogIterator logEnd;

    std::vector<uint64_t> mKeyData;
    std::vector<uint64_t> mValidFromData;
    std::vector<uint64_t> mValidToData;
};

} // namespace deltamain
} // namespace store
} // namespace tell
