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

#include <util/Log.hpp>
#include <util/ScanQuery.hpp>

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

class ColumnMapScanProcessor {
private:
    using LogIterator = Log<OrderedLogImpl>::ConstLogIterator;
    using PageList = std::vector<ColumnMapMainPage*>;

public:
    ColumnMapScanProcessor(const ColumnMapContext& context, const std::shared_ptr<crossbow::allocator>& alloc,
            const PageList& pages, size_t pageIdx, size_t pageEndIdx, const LogIterator& logIter,
            const LogIterator& logEnd, const char* queryBuffer, const std::vector<ScanQuery*>& queryData,
            const Record& record);

    void process();

private:
    void processMainPage(const ColumnMapMainPage* page);

    std::vector<uint64_t> processEntries(const ColumnMapMainEntry* entries, uint32_t count);

    void processInsertRecord(const InsertLogEntry* ptr);

    uint64_t processUpdateRecord(const UpdateLogEntry* ptr, uint64_t baseVersion, uint64_t& validTo);

    const ColumnMapContext& mContext;

    std::shared_ptr<crossbow::allocator> mAllocator;
    const PageList& pages;
    size_t pageIdx;
    size_t pageEndIdx;
    LogIterator logIter;
    LogIterator logEnd;
    ScanQueryBatchProcessor mQuery;
    const Record& mRecord;

    std::vector<char> mResult;
};

} // namespace deltamain
} // namespace store
} // namespace tell
