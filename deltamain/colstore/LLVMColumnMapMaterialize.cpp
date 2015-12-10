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

#include "LLVMColumnMapMaterialize.hpp"

#include "ColumnMapContext.hpp"
#include "LLVMColumnMapUtils.hpp"

namespace tell {
namespace store {
namespace deltamain {

const std::string LLVMColumnMapMaterializeBuilder::FUNCTION_NAME = "materialize";

LLVMColumnMapMaterializeBuilder::LLVMColumnMapMaterializeBuilder(const ColumnMapContext& context, llvm::Module& module,
        llvm::TargetMachine* target)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()),
                FUNCTION_NAME),
          mContext(context),
          mMainPageStructTy(getColumnMapMainPageTy(module.getContext())),
          mHeapEntryStructTy(getColumnMapHeapEntriesTy(module.getContext())) {
    // Set noalias hints (data pointers are not allowed to overlap)
    mFunction->setDoesNotAlias(1);
    mFunction->setOnlyReadsMemory(1);
    mFunction->setDoesNotAlias(3);
}

void LLVMColumnMapMaterializeBuilder::build() {
    auto& record = mContext.record();

    auto mainPage = CreateBitCast(getParam(page), mMainPageStructTy->getPointerTo());

    // Copy the header (null bitmap) if the record has one
    if (record.headerSize() != 0) {
        // auto headerOffset = mainPage->headerOffset;
        auto headerOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(1) });
        headerOffset = CreateZExt(CreateAlignedLoad(headerOffset, 4u), getInt64Ty());

        // -> auto src = page + headerOffset;
        auto src = CreateInBoundsGEP(getParam(page), headerOffset);

        // -> src += idx * record.headerSize();
        src = CreateInBoundsGEP(src, createConstMul(getParam(idx), record.headerSize()));

        // -> memcpy(data, src, record.headerSize());
        CreateMemCpy(getParam(data), src, record.headerSize(), 8u);
    }

    auto count = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(0) });
    count = CreateZExt(CreateAlignedLoad(count, 4u), getInt64Ty());

    // Copy all fixed size fields
    if (record.fixedSizeFieldCount() != 0) {
        // auto fixedOffset = mainPage->fixedOffset;
        auto fixedOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(2) });
        fixedOffset = CreateZExt(CreateAlignedLoad(fixedOffset, 4u), getInt64Ty());

        // -> auto fixedData = page + fixedOffset;
        auto fixedData = CreateInBoundsGEP(getParam(page), fixedOffset);

        for (decltype(record.fixedSizeFieldCount()) i = 0; i < record.fixedSizeFieldCount(); ++i) {
            auto& colMeta = mContext.fixedMetaData()[i];
            auto& rowMeta = record.getFieldMeta(i);
            auto& field = rowMeta.field;
            auto fieldAlignment = field.alignOf();
            auto fieldPtrTy = getFieldPtrTy(field.type());

            // -> auto src = reinterpret_cast<const T*>(fixedData + page->count * fieldOffset) + idx;
            auto src = fixedData;
            if (colMeta.offset != 0) {
                src = CreateInBoundsGEP(src, createConstMul(count, colMeta.offset));
            }
            src = CreateBitCast(src, fieldPtrTy);
            src = CreateInBoundsGEP(src, getParam(idx));

            // -> auto value = *src;
            auto value = CreateAlignedLoad(src, fieldAlignment);

            // -> auto dest = reinterpret_cast<const T*>(data + fieldOffset);
            auto dest = getParam(data);
            if (rowMeta.offset != 0u) {
                dest = CreateInBoundsGEP(dest, getInt64(rowMeta.offset));
            }
            dest = CreateBitCast(dest, fieldPtrTy);

            // -> *dest = value;
            CreateAlignedStore(value, dest, fieldAlignment);
        }
    }

    // Copy all variable size fields in one batch
    if (record.varSizeFieldCount() != 0u) {
        // auto variableOffset = mainPage->variableOffset;
        auto variableOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(3) });
        variableOffset = CreateZExt(CreateAlignedLoad(variableOffset, 4u), getInt64Ty());

        // -> auto src = reinterpret_cast<const ColumnMapHeapEntry*>(page + variableOffset) + idx;
        auto src = CreateInBoundsGEP(getParam(page), variableOffset);
        src = CreateBitCast(src, mHeapEntryStructTy->getPointerTo());
        src = CreateInBoundsGEP(src, getParam(idx));

        // -> auto startOffset = src->offset;
        auto startOffset = CreateInBoundsGEP(src, { getInt64(0), getInt32(0) });
        startOffset = CreateAlignedLoad(startOffset, 8u);

        // The first offset is always the static size of the row record
        {
            auto& rowMeta = record.getFieldMeta(record.fixedSizeFieldCount());

            // -> auto dest = reinterpret_cast<uint32_t*>(data + fieldOffset);
            auto dest = getParam(data);
            if (rowMeta.offset != 0) {
                dest = CreateInBoundsGEP(dest, getInt64(rowMeta.offset));
            }
            dest = CreateBitCast(dest, getInt32PtrTy());

            // -> *dest = record.staticSize();
            CreateAlignedStore(getInt32(record.staticSize()), dest, 4u);
        }

        // Offsets of the remaining fields have to be calculated
        if (record.varSizeFieldCount() > 1) {
            // -> auto offsetCorrection = startOffset + record.staticSize();
            auto offsetCorrection = CreateAdd(startOffset, getInt32(record.staticSize()));
            for (decltype(record.varSizeFieldCount()) i = 1; i < record.varSizeFieldCount(); ++i) {
                // -> src += count;
                src = CreateInBoundsGEP(src, count);

                // -> auto offset = src->offset - offsetCorrection;
                auto offset = CreateInBoundsGEP(src, { getInt64(0), getInt32(0) });
                offset = CreateAlignedLoad(offset, 8u);
                offset = CreateSub(offsetCorrection, offset);

                auto& rowMeta = record.getFieldMeta(record.fixedSizeFieldCount() + i);

                // -> auto dest = reinterpret_cast<uint32_t*>(data + fieldOffset);
                auto dest = CreateInBoundsGEP(getParam(data), getInt64(rowMeta.offset));
                dest = CreateBitCast(dest, getInt32PtrTy());

                // -> *dest = offset;
                CreateAlignedStore(offset, dest, 4u);
            }
        }

        // The last offset is always the size
        {
            // -> auto offset = static_cast<uint32_t>(size);
            auto endOffset = CreateTrunc(getParam(size), getInt32Ty());

            // -> auto dest = reinterpret_cast<uint32_t*>(dest + record.staticSize() - sizeof(uint32_t));
            auto dest = CreateInBoundsGEP(getParam(data), getInt64(record.staticSize() - sizeof(uint32_t)));
            dest = CreateBitCast(dest, getInt32PtrTy());

            // -> *dest = offset;
            CreateAlignedStore(endOffset, dest, 4u);
        }

        // -> auto heapOffset = TELL_PAGE_SIZE - static_cast<uint64_t>(startOffset);
        auto heapOffset = CreateZExt(startOffset, getInt64Ty());
        heapOffset = CreateSub(getInt64(TELL_PAGE_SIZE), heapOffset);

        // -> auto heapSrc = page + heapOffset;
        auto heapSrc = CreateInBoundsGEP(getParam(page), heapOffset);

        // -> auto dest = data + record.staticSize();
        auto dest = CreateInBoundsGEP(getParam(data), getInt64(record.staticSize()));

        // -> auto length = size - record.staticSize();
        auto length = CreateSub(getParam(size), getInt64(record.staticSize()));

        // -> memcpy(dest, heapSrc, length);
        CreateMemCpy(dest, heapSrc, length, 1u);
    }

    // Return
    CreateRetVoid();
}

} // namespace deltamain
} // namespace store
} // namespace tell
