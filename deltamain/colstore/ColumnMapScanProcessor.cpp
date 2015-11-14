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

#include "ColumnMapScanProcessor.hpp"

#include "ColumnMapContext.hpp"
#include "ColumnMapPage.hpp"
#include "ColumnMapRecord.hpp"

#include <deltamain/Table.hpp>

namespace tell {
namespace store {
namespace deltamain {

ColumnMapScan::ColumnMapScan(Table<ColumnMapContext> *table, std::vector<ScanQuery *> queries)
        : QueryBufferScanBase(std::move(queries)),
          mTable(table) {
}

std::vector<std::unique_ptr<ColumnMapScanProcessor>> ColumnMapScan::startScan(size_t numThreads) {
    return mTable->startScan(numThreads, mQueries, mQueryBuffer.get());
}
ColumnMapScanProcessor::ColumnMapScanProcessor(const ColumnMapContext& context, const Record& record,
        const std::vector<ScanQuery*>& queries, const PageList& pages, size_t pageIdx, size_t pageEndIdx,
        const LogIterator& logIter, const LogIterator& logEnd, const char* queryBuffer)
        : QueryBufferScanProcessorBase(record, queries, queryBuffer),
          mContext(context),
          pages(pages),
          pageIdx(pageIdx),
          pageEndIdx(pageEndIdx),
          logIter(logIter),
          logEnd(logEnd) {
}

void ColumnMapScanProcessor::process() {
    for (auto i = pageIdx; i < pageEndIdx; ++i) {
        processMainPage(pages[i]);
    }
    for (auto insIter = logIter; insIter != logEnd; ++insIter) {
        if (!insIter->sealed()) {
            continue;
        }
        processInsertRecord(reinterpret_cast<const InsertLogEntry*>(insIter->data()));
    }
}

void ColumnMapScanProcessor::processMainPage(const ColumnMapMainPage* page) {
    auto entries = page->entryData();
    auto validToData = processEntries(entries, page->count);

    // TODO Evaluate with LLVM

    auto sizeData = page->sizeData();
    for (auto& con : mQueries) {
        for (typename std::remove_const<decltype(page->count)>::type i = 0; i < page->count; ++i) {
            if (mResult[i] == 0u) {
                continue;
            }
            auto length = sizeData[i];
            con.writeRecord(entries[i].key, length, entries[i].version, validToData[i],
                    [this, page, i, length] (char* dest) {
                mContext.materialize(page, i, dest, length);
            });
        }
    }

    mResult.clear();
}

std::vector<uint64_t> ColumnMapScanProcessor::processEntries(const ColumnMapMainEntry* entries, uint32_t count) {
    std::vector<uint64_t> validToData;
    validToData.reserve(count);

    decltype(count) i = 0;
    while (i < count) {
        auto key = entries[i].key;
        auto newest = entries[i].newest.load();
        auto validTo = std::numeric_limits<uint64_t>::max();
        if (newest != 0u) {
            if ((newest & crossbow::to_underlying(NewestPointerTag::INVALID)) != 0x0u) {
                // Set the valid-to version to 0 so the queries ignore the tuple
                validToData.emplace_back(0u);
                for (++i; i < count && entries[i].key == key; ++i) {
                    validToData.emplace_back(0u);
                }
                continue;
            }
            if (auto main = newestMainRecord(newest)) {
                LOG_ERROR("TODO Process relocated main entry");
                std::terminate();

                // Set the valid-to version to 0 so the queries ignore the tuple
                validToData.emplace_back(0u);
                for (++i; i < count && entries[i].key == key; ++i) {
                    validToData.emplace_back(0u);
                }
                continue;
            }

            auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(newest),
                    entries[i].version, validTo);

            // Skip elements with version above lowest version and set the valid-to version to 0 to exclude them from
            // the query processing
            for (; i < count && entries[i].key == key && entries[i].version >= lowestVersion; ++i) {
                validToData.emplace_back(0u);
            }
        }

        // Set valid-to version for every element of the same key to the valid-from version of the previous
        for (; i < count && entries[i].key == key; ++i) {
            validToData.emplace_back(validTo);
            validTo = entries[i].version;
        }
    }
    LOG_ASSERT(validToData.size() == count, "Size of valid-to array does not match the page size");
    return validToData;
}

void ColumnMapScanProcessor::processInsertRecord(const InsertLogEntry* ptr) {
    ConstInsertRecord record(ptr);
    if (!record.valid()) {
        return;
    }

    if (auto main = newestMainRecord(record.newest())) {
        LOG_ERROR("TODO Process relocated main entry");
        std::terminate();
    }

    auto validTo = std::numeric_limits<uint64_t>::max();
    if (record.newest() != 0u) {
        auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(record.newest()),
                record.baseVersion(), validTo);

        if (ptr->version >= lowestVersion) {
            return;
        }
    }
    auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(ptr));
    processRowRecord(ptr->key, ptr->version, validTo, ptr->data(), entry->size() - sizeof(InsertLogEntry));
}

uint64_t ColumnMapScanProcessor::processUpdateRecord(const UpdateLogEntry* ptr, uint64_t baseVersion,
        uint64_t& validTo) {
    UpdateRecordIterator updateIter(ptr, baseVersion);
    for (; !updateIter.done(); updateIter.next()) {
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(updateIter.value()));

        // Check if the entry marks a deletion: Skip element
        if (entry->type() == crossbow::to_underlying(RecordType::DELETE)) {
            validTo = updateIter->version;
            continue;
        }

        processRowRecord(updateIter->key, updateIter->version, validTo, updateIter->data(),
                entry->size() - sizeof(UpdateLogEntry));
        validTo = updateIter->version;
    }
    return updateIter.lowestVersion();
}

} // namespace deltamain
} // namespace store
} // namespace tell
