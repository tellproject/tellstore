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

#include "ColumnMapContext.hpp"

#include "llvm/IR/IRBuilder.h"

#include <tellstore/Record.hpp>

#include <util/PageManager.hpp>

#include <crossbow/logger.hpp>

namespace tell {
namespace store {
namespace deltamain {

namespace {

/**
 * @brief Calculate the additional overhead required by every element in a column map page
 *
 * The overhead consists of the element's header, the size of the element and the heap entries for every variable sized
 * field.
 */
uint32_t calcEntryOverhead(const Record& record) {
    return sizeof(ColumnMapMainEntry) + sizeof(uint32_t) + record.varSizeFieldCount() * sizeof(ColumnMapHeapEntry);
}

/**
 * @brief Calculate the number of elements fitting in one page excluding any variable sized fields
 */
uint32_t calcFixedSizeCapacity(const Record& record) {
    return ColumnMapContext::MAX_DATA_SIZE / (calcEntryOverhead(record) + crossbow::align(record.fixedSize(), 8));
}

} // anonymous namespace

ColumnMapContext::ColumnMapContext(const PageManager& pageManager, const Record& record)
        : mPageData(reinterpret_cast<uintptr_t>(pageManager.data())),
          mEntryOverhead(calcEntryOverhead(record)),
          mFixedSizeCapacity(calcFixedSizeCapacity(record)),
          mFixedSize(record.fixedSize()),
          mVarSizeFieldCount(record.varSizeFieldCount()),
          mLLVMJit(LLVMCodeGenerator::getJIT()) {
    mFieldLengths.reserve(record.fixedSizeFieldCount() + 1);

    uint32_t startOffset = record.headerSize();
    if (startOffset > 0) {
        mFieldLengths.emplace_back(startOffset);
    }
    LOG_ASSERT(record.getFieldMeta(0).second == static_cast<int32_t>(startOffset),
            "First field must point to end of header");

    for (decltype(record.fixedSizeFieldCount()) i = 1; i < record.fixedSizeFieldCount(); ++i) {
        auto endOffset = record.getFieldMeta(i).second;
        LOG_ASSERT(endOffset >= 0, "Offset must be positive");
        LOG_ASSERT(endOffset > static_cast<int32_t>(startOffset), "Offset must be larger than start offset");
        mFieldLengths.emplace_back(static_cast<uint32_t>(endOffset) - startOffset);
        startOffset = static_cast<uint32_t>(endOffset);
    }

    if (record.fixedSizeFieldCount() != 0u) {
        mFieldLengths.emplace_back(static_cast<uint32_t>(record.fixedSize()) - startOffset);
    }

    // Build LLVM
    mMaterialize = LLVMCodeGenerator::generate_colmap_materialize_function(mLLVMJit.get(), *this, "materialize");
}

/*
void ColumnMapContext::materialize(const ColumnMapMainPage *page, uint64_t idx, char *dest, size_t size) const {
    LOG_ASSERT(size > 0, "Tuple must not be of size 0");

    auto recordData = page->recordData();

    // Copy all fixed size fields including the header (null bitmap) if the record has one into the fill page
    for (auto fieldLength : mFieldLengths) {
        memcpy(dest, recordData + idx * fieldLength, fieldLength);
        dest += fieldLength;
        recordData += page->count * fieldLength;
    }

    // Copy all variable size fields in one batch
    if (mVarSizeFieldCount != 0) {
        auto heapEntries = reinterpret_cast<const ColumnMapHeapEntry*>(recordData);
        memcpy(dest, page->heapData() - heapEntries[idx].offset, size - mFixedSize);
    }
}
*/

} // namespace deltamain
} // namespace store
} // namespace tell
