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
#include <util/ScanQuery.hpp>

#include <crossbow/allocator.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {

class Record;

namespace deltamain {

struct InsertLogEntry;
class RowStoreContext;
struct RowStoreMainEntry;
class RowStoreMainPage;
struct UpdateLogEntry;

template <typename Context>
class Table;

class RowStoreScanProcessor;

class RowStoreScan : public LLVMRowScanBase {
public:
    using ScanProcessor = RowStoreScanProcessor;

    RowStoreScan(Table<RowStoreContext>* table, std::vector<ScanQuery*> queries);

    using LLVMRowScanBase::prepareQuery;

    using LLVMRowScanBase::prepareMaterialization;

    std::vector<std::unique_ptr<RowStoreScanProcessor>> startScan(size_t numThreads);

private:
    Table<RowStoreContext>* mTable;
};

class RowStoreScanProcessor : public LLVMRowScanProcessorBase {
public:
    using LogIterator = Log<OrderedLogImpl>::ConstLogIterator;
    using PageList = std::vector<RowStoreMainPage*>;

    RowStoreScanProcessor(const RowStoreContext& context, const Record& record, const std::vector<ScanQuery*>& queries,
            const PageList& pages, size_t pageIdx, size_t pageEndIdx, const LogIterator& logIter,
            const LogIterator& logEnd, RowStoreScan::RowScanFun rowScanFun,
            const std::vector<RowStoreScan::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts);

    void process();

private:
    void processMainRecord(const RowStoreMainEntry* ptr);

    void processInsertRecord(const InsertLogEntry* ptr);

    uint64_t processUpdateRecord(const UpdateLogEntry* ptr, uint64_t baseVersion, uint64_t& validTo);

    const PageList& pages;
    size_t pageIdx;
    size_t pageEndIdx;
    LogIterator logIter;
    LogIterator logEnd;
};

} // namespace deltamain
} // namespace store
} // namespace tell
