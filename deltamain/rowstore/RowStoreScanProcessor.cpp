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

#include "RowStoreScanProcessor.hpp"

#include "RowStorePage.hpp"
#include "RowStoreRecord.hpp"

#include <deltamain/Record.hpp>
#include <deltamain/Table.hpp>

namespace tell {
namespace store {
namespace deltamain {

RowStoreScan::RowStoreScan(Table<RowStoreContext>* table, std::vector<ScanQuery*> queries)
        : LLVMRowScanBase(table->record(), std::move(queries)),
          mTable(table) {
}

std::vector<std::unique_ptr<RowStoreScanProcessor>> RowStoreScan::startScan(size_t numThreads) {
    return mTable->startScan(numThreads, mQueries, mRowScanFun, mRowMaterializeFuns, mScanAst.numConjunct);
}

RowStoreScanProcessor::RowStoreScanProcessor(const RowStoreContext& /* context */, const Record& record,
        const std::vector<ScanQuery*>& queries, const PageList& pages, size_t pageIdx, size_t pageEndIdx,
        const LogIterator& logIter, const LogIterator& logEnd, RowStoreScan::RowScanFun rowScanFun,
        const std::vector<RowStoreScan::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts)
        : LLVMRowScanProcessorBase(record, queries, rowScanFun, rowMaterializeFuns, numConjuncts),
          pages(pages),
          pageIdx(pageIdx),
          pageEndIdx(pageEndIdx),
          logIter(logIter),
          logEnd(logEnd) {
}

void RowStoreScanProcessor::process() {
    for (auto i = pageIdx; i < pageEndIdx; ++i) {
        for (auto& ptr : *pages[i]) {
            processMainRecord(&ptr);
        }
    }
    for (auto insIter = logIter; insIter != logEnd; ++insIter) {
        if (!insIter->sealed()) {
            continue;
        }
        processInsertRecord(reinterpret_cast<const InsertLogEntry*>(insIter->data()));
    }
}

void RowStoreScanProcessor::processMainRecord(const RowStoreMainEntry* ptr) {
    ConstRowStoreRecord record(ptr);

    auto versions = ptr->versionData();
    auto validTo = std::numeric_limits<uint64_t>::max();

    typename std::remove_const<decltype(ptr->versionCount)>::type i = 0;
    if (record.newest() != 0u) {
        if (!record.valid()) {
            return;
        }

        if (auto main = newestMainRecord(record.newest())) {
            return processMainRecord(reinterpret_cast<const RowStoreMainEntry*>(main));
        }

        auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(record.newest()),
                record.baseVersion(), validTo);

        // Skip elements already overwritten by an element in the update log
        for (; i < ptr->versionCount && versions[i] >= lowestVersion; ++i) {
        }
    }

    auto offsets = ptr->offsetData();
    for (; i < ptr->versionCount; ++i) {
        auto sz = offsets[i + 1] - offsets[i];

        // Check if the entry marks a deletion: Skip element
        if (sz == 0) {
            validTo = versions[i];
            continue;
        }

        auto data = reinterpret_cast<const char*>(ptr) + offsets[i];
        processRowRecord(ptr->key, versions[i], validTo, data, sz);
        validTo = versions[i];
    }
}

void RowStoreScanProcessor::processInsertRecord(const InsertLogEntry* ptr) {
    ConstInsertRecord record(ptr);

    auto validTo = std::numeric_limits<uint64_t>::max();

    if (record.newest() != 0u) {
        if (!record.valid()) {
            return;
        }

        if (auto main = newestMainRecord(record.newest())) {
            return processMainRecord(reinterpret_cast<const RowStoreMainEntry*>(main));
        }

        auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(record.newest()),
                record.baseVersion(), validTo);

        if (ptr->version >= lowestVersion) {
            return;
        }
    }

    auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(ptr));
    processRowRecord(ptr->key, ptr->version, validTo, ptr->data(), entry->size() - sizeof(InsertLogEntry));
}

uint64_t RowStoreScanProcessor::processUpdateRecord(const UpdateLogEntry* ptr, uint64_t baseVersion,
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
