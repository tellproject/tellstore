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

#include "HashScanProcessor.hpp"

#include "ChainedVersionRecord.hpp"
#include "LogstructuredMemoryStore.hpp"
#include "Table.hpp"
#include "VersionRecordIterator.hpp"

#include <crossbow/logger.hpp>

#include <boost/config.hpp>

namespace tell {
namespace store {
namespace logstructured {

HashScan::HashScan(Table* table, std::vector<ScanQuery*> queries)
        : LLVMRowScanBase(table->record(), std::move(queries)),
          mTable(table) {
    finalizeRowScan();
}

std::vector<std::unique_ptr<HashScanProcessor>> HashScan::startScan(size_t numThreads) {
    if (numThreads == 0) {
        return {};
    }

    std::vector<std::unique_ptr<HashScanProcessor>> result;
    result.reserve(numThreads);

    auto version = mTable->minVersion();
    auto capacity = mTable->mHashMap.capacity();

    auto step = capacity / numThreads;
    auto mod = capacity % numThreads;
    for (decltype(numThreads) i = 0; i < numThreads; ++i) {
        auto start = i * step + std::min(i, mod);
        auto end = start + step + (i < mod ? 1 : 0);

        result.emplace_back(new HashScanProcessor(*mTable, mQueries, start, end, version, mRowScanFun,
                mRowMaterializeFuns, mScanAst.numConjunct));
    }

    return result;
}

HashScanProcessor::HashScanProcessor(Table& table, const std::vector<ScanQuery*>& queries, size_t start, size_t end,
        uint64_t minVersion, HashScan::RowScanFun rowScanFun,
        const std::vector<HashScan::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts)
        : LLVMRowScanProcessorBase(table.record(), queries, rowScanFun, rowMaterializeFuns, numConjuncts),
          mTable(table),
          mMinVersion(minVersion),
          mStart(start),
          mEnd(end) {
}

void HashScanProcessor::process() {
    mTable.mHashMap.forEach(mStart, mEnd, [this] (uint64_t tableId, uint64_t key, void* ptr) {
        if (tableId != mTable.id()) {
            return;
        }

        auto lastVersion = ChainedVersionRecord::ACTIVE_VERSION;
        for (VersionRecordIterator recIter(mTable, reinterpret_cast<ChainedVersionRecord*>(ptr)); !recIter.done();
                recIter.next()) {
            auto record = recIter.value();

            // Skip element if it is not yet sealed
            auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(record));
            if (BOOST_UNLIKELY(!entry->sealed())) {
                continue;
            }

            // The record iterator might reset itself to the beginning of the version chain if iterator consistency can
            // not be guaranteed. Keep track of the lowest version we have read to prevent scanning tuple more than
            // once.
            if (record->validFrom() >= lastVersion) {
                continue;
            }
            lastVersion = record->validFrom();

            // Skip the element if it is not a data entry (i.e. deletion)
            if (crossbow::from_underlying<VersionRecordType>(entry->type()) != VersionRecordType::DATA) {
                continue;
            }

            auto recordLength = entry->size() - sizeof(ChainedVersionRecord);
            processRowRecord(key, lastVersion, recIter.validTo(), record->data(), recordLength);

            // Check if the iterator reached the element with minimum version. The remaining older elements have to be
            // superseeded by newer elements in any currently valid Snapshot Descriptor.
            if (lastVersion <= mMinVersion) {
                break;
            }
        }
    });
}

void HashScanGarbageCollector::run(const std::vector<Table*>& /* tables */, uint64_t /* minVersion */) {
    // TODO Implement
}

} // namespace logstructured
} // namespace store
} // namespace tell
