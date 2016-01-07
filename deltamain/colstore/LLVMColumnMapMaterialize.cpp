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

    // -> auto mainPage = reinterpret_cast<const ColumnMapMainPage*>(page);
    auto mainPage = CreateBitCast(getParam(page), mMainPageStructTy->getPointerTo());

    // -> auto count = static_cast<uint64_t>(mainPage->count);
    auto count = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(0) });
    count = CreateZExt(CreateAlignedLoad(count, 4u), getInt64Ty());

    // -> auto index = static_cast<uint64_t>(idx);
    auto index = CreateZExt(getParam(idx), getInt64Ty());

    // Copy the header (null bitmap) if the record has one
    if (record.headerSize() != 0) {
        // -> auto headerOffset = static_cast<uint64_t>(mainPage->headerOffset);
        auto headerOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(1) });
        headerOffset = CreateZExt(CreateAlignedLoad(headerOffset, 4u), getInt64Ty());

        // -> auto srcData = page + headerOffset + idx;
        auto srcData = CreateAdd(headerOffset, index);
        srcData = CreateInBoundsGEP(getParam(page), srcData);

        // -> auto srcValue = *srcData;
        auto srcValue = CreateAlignedLoad(srcData, 8u);

        // -> auto destData = data;
        auto destData = getParam(dest);

        // -> *destData = srcValue;
        CreateAlignedStore(srcValue, destData, 8u);

        for (decltype(record.headerSize()) i = 1; i < record.headerSize(); ++i) {
            // -> srcData += count;
            srcData = CreateInBoundsGEP(srcData, count);

            // -> auto srcValue = *srcData;
            auto srcValue = CreateAlignedLoad(srcData, 1u);

            // -> ++destData;
            destData = CreateInBoundsGEP(destData, getInt64(1));

            // -> *destData = srcValue;
            CreateAlignedStore(srcValue, destData, 1u);
        }
    }

    // Copy all fixed size fields
    if (record.fixedSizeFieldCount() != 0) {
        // -> auto fixedOffset = static_cast<uint64_t>(mainPage->fixedOffset);
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
            src = CreateInBoundsGEP(src, index);

            // -> auto value = *src;
            auto value = CreateAlignedLoad(src, fieldAlignment);

            // -> auto dest = reinterpret_cast<const T*>(data + fieldOffset);
            auto destData = getParam(dest);
            if (rowMeta.offset != 0u) {
                destData = CreateInBoundsGEP(destData, getInt64(rowMeta.offset));
            }
            destData = CreateBitCast(destData, fieldPtrTy);

            // -> *dest = value;
            CreateAlignedStore(value, destData, fieldAlignment);
        }
    }

    // -> auto destHeapOffset = record.staticSize();
    llvm::Value* destHeapOffset = getInt32(record.staticSize());

    // Copy all variable size fields in one batch
    if (record.varSizeFieldCount() != 0u) {
        // -> auto variableOffset = static_cast<uint64_t>(mainPage->variableOffset);
        auto variableOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(3) });
        variableOffset = CreateZExt(CreateAlignedLoad(variableOffset, 4u), getInt64Ty());

        // -> auto variableData = reinterpret_cast<const ColumnMapHeapEntry*>(page + variableOffset) + idx;
        auto variableData = CreateInBoundsGEP(getParam(page), variableOffset);
        variableData = CreateBitCast(variableData, mHeapEntryStructTy->getPointerTo());
        variableData = CreateInBoundsGEP(variableData, index);

        // -> auto srcData = variableData;
        auto srcData = variableData;

        // -> auto destData = reinterpret_cast<uint32_t*>(dest + mContext.variableOffset());
        auto destData = getParam(dest);
        if (record.variableOffset() != 0) {
            destData = CreateInBoundsGEP(destData, getInt64(record.variableOffset()));
        }
        destData = CreateBitCast(destData, getInt32PtrTy());

        // -> *destData = destHeapOffset;
        CreateAlignedStore(destHeapOffset, destData, 4u);

        // -> auto srcHeapOffset = srcData->offset;
        auto srcHeapOffset = CreateInBoundsGEP(srcData, { getInt64(0), getInt32(0) });
        srcHeapOffset = CreateAlignedLoad(srcHeapOffset, 8u);

        // -> auto offsetCorrection = srcHeapOffset - destHeapOffset;
        auto offsetCorrection = CreateSub(srcHeapOffset, destHeapOffset);

        // End offsets of all fields have to be calculated
        llvm::Value* offset;
        for (decltype(record.varSizeFieldCount()) i = 0; i < record.varSizeFieldCount(); ++i) {
            if (i + 1 == record.varSizeFieldCount()) {
                // -> srcData = variableData - 1;
                srcData = CreateGEP(variableData, getInt64(-1));
            } else {
                // -> srcData += count;
                srcData = CreateInBoundsGEP(srcData, count);
            }

            // -> auto offset = srcData->offset - offsetCorrection;
            offset = CreateInBoundsGEP(srcData, { getInt64(0), getInt32(0) });
            offset = CreateAlignedLoad(offset, 8u);
            offset = CreateSub(offset, offsetCorrection);

            // -> ++destData;
            destData = CreateInBoundsGEP(destData, getInt64(1));

            // -> *destData = offset;
            CreateAlignedStore(offset, destData, 4u);
        }

        // -> auto srcHeap = page + static_cast<uint64_t>(srcHeapOffset);
        auto srcHeap = CreateInBoundsGEP(getParam(page), CreateZExt(srcHeapOffset, getInt64Ty()));

        // -> auto destHeap = dest + static_cast<uint64_t>(destHeapOffset);
        auto destHeap = CreateInBoundsGEP(getParam(dest), CreateZExt(destHeapOffset, getInt64Ty()));

        // -> auto length = offset - destHeapOffset
        auto length = CreateSub(offset, destHeapOffset);

        // -> memcpy(destHeap, srcHeap, length);
        CreateMemCpy(destHeap, srcHeap, length, 1u);

        // -> destHeapOffset = offset;
        destHeapOffset = offset;
    }

    // -> return destHeapOffset;
    CreateRet(destHeapOffset);
}

} // namespace deltamain
} // namespace store
} // namespace tell
