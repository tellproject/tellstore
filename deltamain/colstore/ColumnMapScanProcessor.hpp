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

#include "ColumnMapPage.hpp"

namespace tell {
namespace store {
namespace deltamain {

class ColumnMapScanProcessor {
private:
    friend class Table;
    using LogIterator = Log<OrderedLogImpl>::ConstLogIterator;
    using PageList = std::vector<char*>;
private: // assigned members
    std::shared_ptr<crossbow::allocator> mAllocator;
    const PageList* pages;
    size_t pageIdx;
    size_t pageEndIdx;
    LogIterator logIter;
    LogIterator logEnd;
    PageManager* pageManager;
    ScanQueryBatchProcessor query;
    const Record* record;
private: // calculated members

    uint64_t currKey;
public:
    ColumnMapScanProcessor(const std::shared_ptr<crossbow::allocator>& alloc,
             const PageList* pages,
             size_t pageIdx,
             size_t pageEndIdx,
             const LogIterator& logIter,
             const LogIterator& logEnd,
             PageManager* pageManager,
             const char* queryBuffer,
             const std::vector<ScanQuery*>& queryData,
             const Record* record);

    void process();
};

} // namespace deltamain
} // namespace store
} // namespace tell
