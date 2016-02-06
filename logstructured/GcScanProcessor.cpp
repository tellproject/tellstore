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

#include "GcScanProcessor.hpp"

#include "ChainedVersionRecord.hpp"
#include "LogstructuredMemoryStore.hpp"
#include "Table.hpp"
#include "VersionRecordIterator.hpp"

#include <crossbow/logger.hpp>

#include <boost/config.hpp>

namespace tell {
namespace store {
namespace logstructured {
namespace {

/**
 * @brief Utilization threshold when to recycle a page in percent
 */
constexpr size_t gGcThreshold = 50;

} // anonymous namespace

GcScan::GcScan(Table* table, std::vector<ScanQuery*> queries)
        : LLVMRowScanBase(table->record(), std::move(queries)),
          mTable(table) {
}

std::vector<std::unique_ptr<GcScanProcessor>> GcScan::startScan(size_t numThreads) {
    if (numThreads == 0) {
        return {};
    }

    std::vector<std::unique_ptr<GcScanProcessor>> result;
    result.reserve(numThreads);

    auto version = mTable->minVersion();
    auto& log = mTable->mLog;

    auto numPages = log.pages();
    auto begin = log.pageBegin();
    auto end = log.pageEnd();

    auto mod = numPages % numThreads;
    auto iter = begin;
    for (decltype(numThreads) i = 1; i < numThreads; ++i) {
        auto step = numPages / numThreads + (i < mod ? 1 : 0);
        // Increment the page iterator by step pages (but not beyond the end page)
        for (decltype(step) j = 0; j < step && iter != end; ++j, ++iter) {
        }

        result.emplace_back(new GcScanProcessor(*mTable, mQueries, begin, iter, version, mRowScanFun,
                mRowMaterializeFuns, mScanAst.numConjunct));
        begin = iter;
    }

    // The last scan takes the remaining pages
    result.emplace_back(new GcScanProcessor (*mTable, mQueries, begin, end, version, mRowScanFun, mRowMaterializeFuns,
            mScanAst.numConjunct));

    return result;
}

GcScanProcessor::GcScanProcessor(Table& table, const std::vector<ScanQuery*>& queries, const PageIterator& begin,
        const PageIterator& end, uint64_t minVersion, GcScan::RowScanFun rowScanFun,
        const std::vector<GcScan::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts)
        : LLVMRowScanProcessorBase(table.record(), queries, rowScanFun, rowMaterializeFuns, numConjuncts),
          mTable(table),
          mMinVersion(minVersion),
          mPagePrev(begin),
          mPageIt(begin),
          mPageEnd(end),
          mEntryIt(mPageIt == mPageEnd ? LogPage::EntryIterator() : mPageIt->begin()),
          mEntryEnd(mPageIt == mPageEnd ? LogPage::EntryIterator() : mPageIt->end()),
          mRecyclingHead(nullptr),
          mRecyclingTail(nullptr),
          mGarbage(0x0u),
          mSealed(false),
          mRecycle(false) {
}

void GcScanProcessor::process() {
    // Abort if the processor already is at the end
    if (mPageIt == mPageEnd) {
        return;
    }

    // Advance to the next page if the first page contains no entries
    if (mEntryIt == mEntryEnd && !advancePage()) {
        return;
    }

    do {
        if (BOOST_UNLIKELY(!mEntryIt->sealed())) {
            LOG_ASSERT(!mRecycle, "Recycling page even though not all entries are sealed");
            mSealed = false;
            continue;
        }
        LOG_ASSERT(mEntryIt->size() >= sizeof(ChainedVersionRecord), "Log record is smaller than record header");

        auto record = reinterpret_cast<ChainedVersionRecord*>(mEntryIt->data());

        auto context = record->mutableData();
        if (context.isInvalid()) {
            // The element is already marked as invalid - Increase the garbage counter
            mGarbage += mEntryIt->entrySize();
            continue;
        }

        auto type = crossbow::from_underlying<VersionRecordType>(mEntryIt->type());
        if (context.validTo() <= mMinVersion) {
            // No version can read the current element - Mark it as invalid and increase the garbage counter
#ifdef NDEBUG
            record->invalidate();
#else
            auto res = record->tryInvalidate(context, nullptr);
            LOG_ASSERT(res, "Invalidating expired element failed");
#endif
            mGarbage += mEntryIt->entrySize();
            continue;
        } else if ((type == VersionRecordType::DELETION) && (record->validFrom() <= mMinVersion)) {
            // Try to mark the deletion as invalid and set the next pointer to null
            // This basically truncates the version list and marks the deletion entry as deleted in the version history
            // Because the entry is still alive (i.e. can be accessed by other transactions) we have to use a CAS to
            // invalidate the entry
            if (!record->tryInvalidate(context, nullptr)) {
                continue;
            }
            mGarbage += mEntryIt->entrySize();

            // Iterate over the whole version list for this key, this ensures the removal of the invalid deletion entry
            for (VersionRecordIterator recIter(mTable, record->key()); !recIter.done(); recIter.next()) {
            }
            continue;
        }

        if (mRecycle) {
            recycleEntry(record, mEntryIt->size(), mEntryIt->type());
        }

        // Skip the element if it is not a data entry (i.e. deletion)
        if (type != VersionRecordType::DATA) {
            continue;
        }

        // Process the element
        auto recordLength = mEntryIt->size() - sizeof(ChainedVersionRecord);
        processRowRecord(record->key(), record->validFrom(), context.validTo(), record->data(), recordLength);
    } while (advanceEntry());

    // Append recycled entries to the log
    if (mRecyclingHead != nullptr) {
        LOG_ASSERT(mRecyclingTail, "Recycling tail is null despite head being non null");
        mTable.mLog.appendPage(mRecyclingHead, mRecyclingTail);
    }
}

bool GcScanProcessor::advanceEntry() {
    // Advance the iterator to the next entry
    if (++mEntryIt != mEntryEnd) {
        return true;
    }

    // Advance to next page
    return advancePage();
}

bool GcScanProcessor::advancePage() {
    do {
        // Advance to next page
        if (mRecycle) {
            ++mPageIt;
            mTable.mLog.erase(mPagePrev.operator->(), mPageIt.operator->());
        } else {
            // Only store the garbage statistic when every entry in the page was sealed
            if (mSealed) {
                mPageIt->context().store(mGarbage);
            }
            mPagePrev = mPageIt++;
        }

        if (mPageIt == mPageEnd) {
            return false;
        }
        mEntryIt = mPageIt->begin();
        mEntryEnd = mPageIt->end();

        // Retrieve usage statistics of the current page
        mGarbage = 0x0u;
        uint32_t offset;
        std::tie(offset, mSealed) = mPageIt->offsetAndSealed();
        auto currentGarbage = mPageIt->context().load();
        auto size = (currentGarbage >= offset ? 0u : offset - currentGarbage);
        mRecycle = (mSealed && ((size * 100) / LogPage::MAX_DATA_SIZE < gGcThreshold));
    } while (mEntryIt == mEntryEnd);

    return true;
}

void GcScanProcessor::recycleEntry(ChainedVersionRecord* oldElement, uint32_t size, uint32_t type) {
    if (mRecyclingHead == nullptr) {
        mRecyclingHead = mTable.mLog.acquirePage();
        if (mRecyclingHead == nullptr) {
            LOG_ERROR("PageManager ran out of space");
            mRecycle = false;
            return;
        }
        mRecyclingTail = mRecyclingHead;
    }

    auto newEntry = mRecyclingHead->append(size, type);
    if (newEntry == nullptr) {
        auto newHead = mTable.mLog.acquirePage();
        if (newHead == nullptr) {
            LOG_ERROR("PageManager ran out of space");
            mRecycle = false;
            return;
        }
        newHead->next().store(mRecyclingHead);
        mRecyclingHead->seal();
        mRecyclingHead = newHead;
        newEntry = mRecyclingHead->append(size, type);
        LOG_ASSERT(newEntry, "Unable to allocate entry on fresh page");
    }

    auto newElement = new (newEntry->data()) ChainedVersionRecord(oldElement->key(), oldElement->validFrom());
    memcpy(newElement->data(), oldElement->data(), size - sizeof(ChainedVersionRecord));

    if (!replaceElement(oldElement, newElement)) {
        newElement->invalidate();
    }

    newEntry->seal();
}

bool GcScanProcessor::replaceElement(ChainedVersionRecord* oldElement, ChainedVersionRecord* newElement) {
    LOG_ASSERT(oldElement->key() == newElement->key(), "Keys do not match");

    // Search for the old element in the version list - if it was not found it has to be invalidated by somebody else
    VersionRecordIterator recIter(mTable, oldElement->key());
    if (!recIter.find(oldElement)) {
        LOG_ASSERT(oldElement->mutableData().isInvalid(), "Old element not in version list but not invalid");
        return false;
    }

    // Replace can fail because the next pointer or validTo version of the current element has changed or it was
    // invalidated by someone else - if it was invalidated then the iterator will point to a different element
    while (!recIter.replace(newElement)) {
        if (recIter.value() != oldElement) {
            LOG_ASSERT(oldElement->mutableData().isInvalid(), "Old element not in version list but not invalid");
            return false;
        }
    }

    // A successful replace only guarantees that the old element was invalidated (and replaced with the new element) but
    // the old element could still be in the version list - Traverse the iterator until the new element is reached (this
    // ensures all previous elements were valid at some time and we can safely reuse the memory of the old element)
    if (!recIter.find(newElement)) {
        LOG_ASSERT(newElement->mutableData().isInvalid(), "New element not in version list but not invalid");
    }
    return true;
}

void GcScanGarbageCollector::run(const std::vector<Table*>& tables, uint64_t /* minVersion */) {
    for (auto i : tables) {
        LOG_TRACE("Starting garbage collection on table %1%", i->tableId());
        if (mStorage.scan(i->tableId(), nullptr)) {
            LOG_ERROR("Unable to start Garbage Collection scan");
            return;
        }
    }
}

} // namespace logstructured
} // namespace store
} // namespace tell
