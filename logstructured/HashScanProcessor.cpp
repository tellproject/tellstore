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

std::vector<HashScanProcessor> HashScanProcessor::startScan(Table& table, size_t numThreads, const char* queryBuffer,
        const std::vector<ScanQuery*>& queries) {
    if (numThreads == 0) {
        return {};
    }

    std::vector<HashScanProcessor> result;
    result.reserve(numThreads);

    auto version = table.minVersion();
    auto capacity = table.mHashMap.capacity();

    auto step = capacity / numThreads;
    auto mod = capacity % numThreads;
    for (decltype(numThreads) i = 0; i < numThreads; ++i) {
        auto start = i * step + std::min(i, mod);
        auto end = start + step + (i < mod ? 1 : 0);

        result.emplace_back(table, start, end, queryBuffer, queries, version);
    }

    return result;
}

HashScanProcessor::HashScanProcessor(Table& table, size_t start, size_t end, const char* queryBuffer,
        const std::vector<ScanQuery*>& queryData, uint64_t minVersion)
        : mTable(table),
          mQueries(queryBuffer, queryData),
          mMinVersion(minVersion),
          mStart(start),
          mEnd(end) {
}

HashScanProcessor::HashScanProcessor(HashScanProcessor&& other)
        : mTable(other.mTable),
          mQueries(std::move(other.mQueries)),
          mMinVersion(other.mMinVersion),
          mStart(other.mStart),
          mEnd(other.mEnd) {
    other.mStart = 0x0u;
    other.mEnd = 0x0u;
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
            mQueries.processRecord(mTable.record(), key, record->data(), recordLength, lastVersion, recIter.validTo());

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
