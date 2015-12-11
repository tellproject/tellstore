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

#include "LLVMColumnMapProjection.hpp"

#include "ColumnMapContext.hpp"
#include "LLVMColumnMapUtils.hpp"

#include <util/ScanQuery.hpp>

namespace tell {
namespace store {
namespace deltamain {

const std::string LLVMColumnMapProjectionBuilder::FUNCTION_NAME = "columnMaterialize.";

LLVMColumnMapProjectionBuilder::LLVMColumnMapProjectionBuilder(const ColumnMapContext& context, llvm::Module& module,
        llvm::TargetMachine* target, uint32_t index)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()),
                createFunctionName(index)),
          mContext(context),
          mMainPageStructTy(getColumnMapMainPageTy(module.getContext())),
          mHeapEntryStructTy(getColumnMapHeapEntriesTy(module.getContext())) {
    // Set noalias hints (data pointers are not allowed to overlap)
    mFunction->setDoesNotAlias(1);
    mFunction->setOnlyReadsMemory(1);
    mFunction->setDoesNotAlias(4);
}

void LLVMColumnMapProjectionBuilder::build(ScanQuery* query) {
    auto& srcRecord = mContext.record();
    auto& destRecord = query->record();

    // -> auto mainPage = reinterpret_cast<const ColumnMapMainPage*>(page);
    auto mainPage = CreateBitCast(getParam(page), mMainPageStructTy->getPointerTo());

    // -> auto index = static_cast<uint64_t>(idx);
    auto index = CreateZExt(getParam(idx), getInt64Ty());

    if (destRecord.headerSize() != 0u) {
        // -> memset(dest, 0, destRecord.headerSize());
        CreateMemSet(getParam(dest), getInt8(0), destRecord.headerSize(), 8u);

        // -> auto headerOffset = static_cast<uint64_t>(mainPage->headerOffset);
        auto headerOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(1) });
        headerOffset = CreateZExt(CreateAlignedLoad(headerOffset, 4u), getInt64Ty());

        // -> auto headerData = page + headerOffset + index * destRecord.headerSize();
        auto headerData = CreateInBoundsGEP(getParam(page), headerOffset);
        headerData = CreateInBoundsGEP(headerData, createConstMul(index, destRecord.headerSize()));

        auto i = query->projectionBegin();
        for (decltype(destRecord.fieldCount()) destFieldIdx = 0u; destFieldIdx < destRecord.fieldCount();
                ++i, ++destFieldIdx) {
            auto srcFieldIdx = *i;
            auto& field = srcRecord.getFieldMeta(srcFieldIdx).field;
            if (field.isNotNull()) {
                continue;
            }

            auto srcIdx = srcFieldIdx / 8u;
            auto srcBitIdx = srcFieldIdx % 8u;
            uint8_t srcMask = (0x1u << srcBitIdx);

            // -> auto srcNullBitmap = *(headerData + srcIdx) & srcMask;
            auto srcNullBitmap = headerData;
            if (srcIdx != 0) {
                srcNullBitmap = CreateInBoundsGEP(srcNullBitmap, getInt64(srcIdx));
            }
            srcNullBitmap = CreateAlignedLoad(srcNullBitmap, 1u);
            srcNullBitmap = CreateAnd(srcNullBitmap, getInt8(srcMask));

            auto destIdx = destFieldIdx / 8u;
            auto destBitIdx = destFieldIdx % 8u;

            if (destBitIdx > srcBitIdx) {
                // -> srcNullBitmap = srcNullBitmap << (destBitIdx - srcBitIdx);
                srcNullBitmap = CreateShl(srcNullBitmap, getInt64(destBitIdx - srcBitIdx));
            } else if (destBitIdx < srcBitIdx) {
                // -> srcNullBitmap = srcNullBitmap >> (srcBitIdx - destBitIdx);
                srcNullBitmap = CreateLShr(srcNullBitmap, getInt64(srcBitIdx - destBitIdx));
            }

            // -> auto destNullBitmapPtr = dest + destIdx;
            auto destNullBitmapPtr = getParam(dest);
            if (destIdx != 0) {
                destNullBitmapPtr = CreateInBoundsGEP(destNullBitmapPtr, getInt64(destIdx));
            }

            // -> auto destNullBitmap = *destNullBitmapPtr | srcNullBitmap;
            llvm::Value* destNullBitmap = CreateAlignedLoad(destNullBitmapPtr, 1u);
            destNullBitmap = CreateOr(destNullBitmapPtr, srcNullBitmap);

            // -> *destNullBitmapPtr = destNullBitmap;
            CreateAlignedStore(destNullBitmap, destNullBitmapPtr, 1u);
        }
    }

    // -> auto count = static_cast<uint64_t>(mainPage->count);
    auto count = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(0) });
    count = CreateZExt(CreateAlignedLoad(count, 4u), getInt64Ty());

    auto i = query->projectionBegin();
    if (destRecord.fixedSizeFieldCount() != 0) {
        // auto fixedOffset = static_cast<uint64_t>(mainPage->fixedOffset);
        auto fixedOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(2) });
        fixedOffset = CreateZExt(CreateAlignedLoad(fixedOffset, 4u), getInt64Ty());

        // -> auto fixedData = page + fixedOffset;
        auto fixedData = CreateInBoundsGEP(getParam(page), fixedOffset);

        for (decltype(destRecord.fixedSizeFieldCount()) destFieldIdx = 0u;
                destFieldIdx < destRecord.fixedSizeFieldCount(); ++i, ++destFieldIdx) {
            auto srcFieldIdx = *i;
            auto& srcMeta = mContext.fixedMetaData()[srcFieldIdx];
            auto& destMeta = destRecord.getFieldMeta(destFieldIdx);
            auto& field = destMeta.field;
            LOG_ASSERT(field.isFixedSized(), "Field must be fixed size");

            auto fieldAlignment = field.alignOf();
            auto fieldPtrType = getFieldPtrTy(field.type());

            // -> auto srcData = reinterpret_cast<const T*>(fixedData + srcMeta.offset) + index;
            auto srcData = fixedData;
            if (srcMeta.offset != 0) {
                srcData = CreateInBoundsGEP(srcData, createConstMul(count, srcMeta.offset));
            }
            srcData = CreateBitCast(srcData, fieldPtrType);
            srcData = CreateInBoundsGEP(srcData, index);

            // -> auto value = *srcData;
            auto value = CreateAlignedLoad(srcData, fieldAlignment);

            // -> auto destData = reinterpret_cast<const T*>(dest + destMeta.offset);
            auto destData = getParam(dest);
            if (destMeta.offset != 0) {
                destData = CreateInBoundsGEP(destData, getInt64(destMeta.offset));
            }
            destData = CreateBitCast(destData, fieldPtrType);

            // -> *destData = value;
            CreateAlignedStore(value, destData, fieldAlignment);
        }
    }

    // -> auto destHeapOffset = destRecord.staticSize();
    llvm::Value* destHeapOffset = getInt32(destRecord.staticSize());

    if (destRecord.varSizeFieldCount() != 0) {
        auto srcFieldIdx = srcRecord.fixedSizeFieldCount();
        decltype(destRecord.varSizeFieldCount()) destFieldIdx = 0;

        auto& destMeta = destRecord.getFieldMeta(destRecord.fixedSizeFieldCount());
        LOG_ASSERT(!destMeta.field.isFixedSized(), "Field must be variable size");

        // auto variableOffset = static_cast<uint64_t>(mainPage->variableOffset);
        auto variableOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(3) });
        variableOffset = CreateZExt(CreateAlignedLoad(variableOffset, 4u), getInt64Ty());

        // -> auto variableData = reinterpret_cast<const ColumnMapHeapEntry*>(page + variableOffset) + idx;
        auto variableData = CreateInBoundsGEP(getParam(page), variableOffset);
        variableData = CreateBitCast(variableData, mHeapEntryStructTy->getPointerTo());
        variableData = CreateInBoundsGEP(variableData, index);

        // -> auto srcData = variableData;
        auto srcData = variableData;

        // -> auto destData = reinterpret_cast<uint32_t*>(dest + destMeta.offset);
        auto destData = getParam(dest);
        if (destMeta.offset != 0) {
            destData = CreateInBoundsGEP(destData, getInt64(destMeta.offset));
        }
        destData = CreateBitCast(destData, getInt32PtrTy());

        // -> *destData = destHeapOffset;
        CreateAlignedStore(destHeapOffset, destData, 4u);

        do {
            if (*i != srcFieldIdx) {
                auto step = *i - srcFieldIdx;
                // -> srcData += count * (*i - srcFieldIdx);
                srcData = CreateInBoundsGEP(srcData, createConstMul(count, step));
                srcFieldIdx = *i;
            }

            // -> auto srcHeapOffset = srcData->offset;
            auto srcHeapOffset = CreateInBoundsGEP(srcData, { getInt64(0), getInt32(0) });
            srcHeapOffset = CreateAlignedLoad(srcHeapOffset, 8u);

            // -> auto offsetCorrection = srcHeapOffset + destHeapOffset;
            auto offsetCorrection = CreateAdd(srcHeapOffset, destHeapOffset);

            llvm::Value* offset;
            do {
                ++i;

                // Step to offset of the following field (or to the last field of the previous element) to get the end
                // offset
                ++srcFieldIdx;
                if (srcFieldIdx == srcRecord.fieldCount()) {
                    // -> srcData = variableData - 1;
                    srcData = CreateGEP(variableData, getInt64(-1));
                } else {
                    // -> srcData += count;
                    srcData = CreateInBoundsGEP(srcData, count);
                }

                // -> auto offset = offsetCorrection - srcData->offset;
                offset = CreateInBoundsGEP(srcData, { getInt64(0), getInt32(0) });
                offset = CreateAlignedLoad(offset, 8u);
                offset = CreateSub(offsetCorrection, offset);

                // -> ++destData;
                ++destFieldIdx;
                destData = CreateInBoundsGEP(destData, getInt64(1));

                // -> *destData = offset;
                CreateAlignedStore(offset, destData, 4u);
            } while (destFieldIdx < destRecord.varSizeFieldCount() && *i == srcFieldIdx);


            // -> srcHeapOffset = TELL_PAGE_SIZE - static_cast<uint64_t>(srcHeapOffset);
            srcHeapOffset = CreateZExt(srcHeapOffset, getInt64Ty());
            srcHeapOffset = CreateSub(getInt64(TELL_PAGE_SIZE), srcHeapOffset);

            // -> auto srcHeap = page + srcHeapOffset;
            auto srcHeap = CreateInBoundsGEP(getParam(page), srcHeapOffset);

            // -> auto destHeap = dest + destHeapOffset;
            auto destHeap = CreateInBoundsGEP(getParam(dest), destHeapOffset);

            // -> auto length = offset - destHeapOffset
            auto length = CreateSub(offset, destHeapOffset);

            // -> memcpy(destHeap, srcHeap, length);
            CreateMemCpy(destHeap, srcHeap, length, 1u);

            // -> destHeapOffset = offset;
            destHeapOffset = offset;
        } while (destFieldIdx < destRecord.varSizeFieldCount());
    }

    // -> return destHeapOffset;
    CreateRet(destHeapOffset);
}

} // namespace deltamain
} // namespace store
} // namespace tell
