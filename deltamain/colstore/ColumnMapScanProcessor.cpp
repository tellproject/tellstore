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
#include "LLVMColumnMapScan.hpp"
#include "LLVMColumnMapUtils.hpp"

#include <deltamain/Table.hpp>

#include <util/LLVMBuilder.hpp>

namespace tell {
namespace store {
namespace deltamain {

ColumnMapScan::ColumnMapScan(Table<ColumnMapContext>* table, std::vector<ScanQuery*> queries)
        : LLVMRowScanBase(table->record(), std::move(queries)),
          mTable(table),
          mColumnScanFun(nullptr) {
    auto& context = table->context();
    auto targetMachine = mCompiler.getTargetMachine();
    LLVMColumnMapScanBuilder::createFunction(context, mCompilerModule, targetMachine, mScanAst);

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        auto q = mQueries[i];
        switch (q->queryType()) {
        case ScanQueryType::PROJECTION: {
            LLVMColumnMapProjectionBuilder::createFunction(context, mCompilerModule, targetMachine, i, q);
        } break;

        case ScanQueryType::AGGREGATION: {
            LLVMColumnMapAggregationBuilder::createFunction(context, mCompilerModule, targetMachine, i, q);
        } break;
        default:
            break;
        }
    }

    finalizeRowScan();

    mColumnScanFun = mCompiler.findFunction<ColumnScanFun>(LLVMColumnMapScanBuilder::FUNCTION_NAME);

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        switch (mQueries[i]->queryType()) {
        case ScanQueryType::FULL: {
            mColumnProjectionFuns.emplace_back(context.materializeFunction());
            mColumnAggregationFuns.emplace_back(nullptr);
        } break;

        case ScanQueryType::PROJECTION: {
            auto fun = mCompiler.findFunction<ColumnProjectionFun>(
                    LLVMColumnMapProjectionBuilder::createFunctionName(i));
            mColumnProjectionFuns.emplace_back(fun);
            mColumnAggregationFuns.emplace_back(nullptr);
        } break;

        case ScanQueryType::AGGREGATION: {
            auto fun = mCompiler.findFunction<ColumnAggregationFun>(
                    LLVMColumnMapAggregationBuilder::createFunctionName(i));
            mColumnAggregationFuns.emplace_back(fun);
            mColumnProjectionFuns.emplace_back(nullptr);
        } break;
        }
    }
}

std::vector<std::unique_ptr<ColumnMapScanProcessor>> ColumnMapScan::startScan(size_t numThreads) {
    return mTable->startScan(numThreads, mQueries, mColumnScanFun, mColumnProjectionFuns, mColumnAggregationFuns,
            mRowScanFun, mRowMaterializeFuns, mScanAst.numConjunct);
}

ColumnMapScanProcessor::ColumnMapScanProcessor(const ColumnMapContext& context, const Record& record,
        const std::vector<ScanQuery*>& queries, const PageList& pages, size_t pageIdx, size_t pageEndIdx,
        const LogIterator& logIter, const LogIterator& logEnd, ColumnMapScan::ColumnScanFun columnScanFun,
        const std::vector<ColumnMapScan::ColumnProjectionFun>& columnProjectionFuns,
        const std::vector<ColumnMapScan::ColumnAggregationFun>& columnAggregationFuns,
        ColumnMapScan::RowScanFun rowScanFun, const std::vector<ColumnMapScan::RowMaterializeFun>& rowMaterializeFuns,
        uint32_t numConjuncts)
        : LLVMRowScanProcessorBase(record, queries, rowScanFun, rowMaterializeFuns, numConjuncts),
          mContext(context),
          mColumnScanFun(columnScanFun),
          mColumnProjectionFuns(columnProjectionFuns),
          mColumnAggregationFuns(columnAggregationFuns),
          pages(pages),
          pageIdx(pageIdx),
          pageEndIdx(pageEndIdx),
          logIter(logIter),
          logEnd(logEnd) {
}

void ColumnMapScanProcessor::process() {
    for (auto i = pageIdx; i < pageEndIdx; ++i) {
        processMainPage(pages[i], 0, pages[i]->count);
    }

    auto insIter = logIter;
    while (insIter != logEnd) {
        if (!insIter->sealed()) {
            ++insIter;
            continue;
        }

        auto ptr = reinterpret_cast<const InsertLogEntry*>(insIter->data());
        ConstInsertRecord record(ptr);
        if (!record.valid()) {
            ++insIter;
            continue;
        }

        if (auto relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(record.newest()))) {
            auto relocatedPage = mContext.pageFromEntry(relocated);
            auto relocatedStartIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
            auto relocatedEndIdx = relocatedStartIdx;

            for (++insIter; insIter != logEnd; ++insIter) {
                if (!insIter->sealed()) {
                    continue;
                }

                ptr = reinterpret_cast<const InsertLogEntry*>(insIter->data());
                record = ConstInsertRecord(ptr);

                relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(record.newest()));
                if (relocated) {
                    if (mContext.pageFromEntry(relocated) != relocatedPage) {
                        break;
                    }
                    relocatedEndIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
                } else if (!record.valid()) {
                    continue;
                } else {
                    break;
                }
            }

            auto relocatedEntries = relocatedPage->entryData();
            auto key = relocatedEntries[relocatedEndIdx].key;
            for (++relocatedEndIdx; relocatedEndIdx < relocatedPage->count && relocatedEntries[relocatedEndIdx].key == key; ++relocatedEndIdx);

            processMainPage(relocatedPage, relocatedStartIdx, relocatedEndIdx);

            continue;
        }

        auto validTo = std::numeric_limits<uint64_t>::max();
        if (record.newest() != 0u) {
            auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(record.newest()),
                    record.baseVersion(), validTo);

            if (ptr->version >= lowestVersion) {
                ++insIter;
                continue;
            }
        }
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(ptr));
        processRowRecord(ptr->key, ptr->version, validTo, ptr->data(), entry->size() - sizeof(InsertLogEntry));
        ++insIter;
    }
}

void ColumnMapScanProcessor::processMainPage(const ColumnMapMainPage* page, uint64_t startIdx, uint64_t endIdx) {
    mKeyData.resize(page->count, 0u);
    mValidFromData.resize(page->count, 0u);
    mValidToData.resize(page->count, 0u);
    auto resultSize = mNumConjuncts * page->count;
    if (mResult.size() < resultSize) {
        mResult.resize(resultSize, 0u);
    }

    auto entries = page->entryData();
    auto sizeData = page->sizeData();

    auto i = startIdx;
    while (i < endIdx) {
        auto key = entries[i].key;
        auto newest = entries[i].newest.load();
        auto validTo = std::numeric_limits<uint64_t>::max();
        if (newest != 0u) {
            if ((newest & crossbow::to_underlying(NewestPointerTag::INVALID)) != 0x0u) {
                // Skip to element with next key
                auto j = i;
                for (++i; i < endIdx && entries[i].key == key; ++i);
                if (startIdx == j) {
                    startIdx = i;
                }
                continue;
            }
            if (auto relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(newest))) {
                if (i > startIdx) {
                    evaluateMainQueries(page, startIdx, i);
                }

                auto relocatedPage = mContext.pageFromEntry(relocated);
                auto relocatedStartIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
                auto relocatedEndIdx = relocatedStartIdx;

                while (true) {
                    for (++i; i < endIdx && entries[i].key == key; ++i);
                    if (i >= endIdx) {
                        break;
                    }

                    key = entries[i].key;
                    newest = entries[i].newest.load();

                    relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(newest));
                    if (relocated) {
                        if (mContext.pageFromEntry(relocated) != relocatedPage) {
                            break;
                        }
                        relocatedEndIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
                    } else if ((newest & crossbow::to_underlying(NewestPointerTag::INVALID)) != 0x0u) {
                        continue;
                    } else {
                        break;
                    }
                }

                auto relocatedEntries = relocatedPage->entryData();
                key = relocatedEntries[relocatedEndIdx].key;
                for (++relocatedEndIdx; relocatedEndIdx < relocatedPage->count && relocatedEntries[relocatedEndIdx].key == key; ++relocatedEndIdx);

                processMainPage(relocatedPage, relocatedStartIdx, relocatedEndIdx);

                if (i >= endIdx) {
                    return;
                }

                mKeyData.resize(page->count, 0u);
                mValidFromData.resize(page->count, 0u);
                mValidToData.resize(page->count, 0u);

                startIdx = i;
                continue;
            }

            auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(newest),
                    entries[i].version, validTo);

            // Skip elements with version above lowest version and set the valid-to version to 0 to exclude them from
            // the query processing
            auto j = i;
            for (; i < endIdx && entries[i].key == key && entries[i].version >= lowestVersion; ++i);
            if (startIdx == j) {
                startIdx = i;
            }
        }

        // Set valid-to version for every element of the same key to the valid-from version of the previous
        // If the element marks a deletion set the valid-to version to 0 to exclude them from the query processing.
        for (; i < endIdx && entries[i].key == key; ++i) {
            if (sizeData[i] != 0) {
                mKeyData[i] = key;
                mValidFromData[i] = entries[i].version;
                mValidToData[i] = validTo;
            }
            validTo = entries[i].version;
        }
    }
    if (startIdx < endIdx) {
        evaluateMainQueries(page, startIdx, endIdx);
    }
}

void ColumnMapScanProcessor::evaluateMainQueries(const ColumnMapMainPage* page, uint64_t startIdx, uint64_t endIdx) {
    LOG_ASSERT(mKeyData.size() == page->count, "Size of key array does not match the page size");
    LOG_ASSERT(mValidFromData.size() == page->count, "Size of valid-from array does not match the page size");
    LOG_ASSERT(mValidToData.size() == page->count, "Size of valid-to array does not match the page size");

    mColumnScanFun(&mKeyData.front(), &mValidFromData.front(), &mValidToData.front(),
            reinterpret_cast<const char*>(page), startIdx, endIdx, &mResult.front());

    auto entries = page->entryData();
    auto sizeData = page->sizeData();
    auto result = &mResult.front();
    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        switch (mQueries[i].data()->queryType()) {
        case ScanQueryType::FULL: {
        case ScanQueryType::PROJECTION:
            auto fun = mColumnProjectionFuns[i];
            for (decltype(startIdx) j = startIdx; j < endIdx; ++j) {
                if (result[j] == 0u) {
                    continue;
                }
                auto length = sizeData[j];
                mQueries[i].writeRecord(entries[j].key, length, entries[j].version, mValidToData[j],
                        [fun, page, j, length] (char* dest) {
                    return fun(reinterpret_cast<const char*>(page), j, length, dest);
                });
            }
        } break;

        case ScanQueryType::AGGREGATION: {
            mColumnAggregationFuns[i](reinterpret_cast<const char*>(page), startIdx, endIdx, result,
                    mQueries[i].mBuffer + 8);
        } break;

        }
        result += page->count;
    }

    mKeyData.clear();
    mValidFromData.clear();
    mValidToData.clear();
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
