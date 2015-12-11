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

#include "LLVMRowProjection.hpp"

#include <util/ScanQuery.hpp>

#include <llvm/IR/Module.h>

namespace tell {
namespace store {

const std::string LLVMRowProjectionBuilder::FUNCTION_NAME = "rowMaterialize.";

LLVMRowProjectionBuilder::LLVMRowProjectionBuilder(const Record& record, llvm::Module& module,
        llvm::TargetMachine* target, uint32_t index)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()),
                createFunctionName(index)),
          mRecord(record) {
    // Set noalias hints (data pointers are not allowed to overlap)
    mFunction->setDoesNotAlias(1);
    mFunction->setOnlyReadsMemory(1);
    mFunction->setDoesNotAlias(3);
}

void LLVMRowProjectionBuilder::build(ScanQuery* query) {
    auto& destRecord = query->record();
    if (destRecord.headerSize() != 0u) {
        // -> memset(dest, 0, destRecord.headerSize());
        CreateMemSet(getParam(dest), getInt8(0), destRecord.headerSize(), 8u);

        auto i = query->projectionBegin();
        for (decltype(destRecord.fieldCount()) destFieldIdx = 0u; destFieldIdx < destRecord.fieldCount();
                ++i, ++destFieldIdx) {
            auto srcFieldIdx = *i;
            auto& field = mRecord.getFieldMeta(srcFieldIdx).field;
            if (field.isNotNull()) {
                continue;
            }

            auto srcIdx = srcFieldIdx / 8u;
            auto srcBitIdx = srcFieldIdx % 8u;
            uint8_t srcMask = (0x1u << srcBitIdx);

            // -> auto srcNullBitmap = *(src + srcIdx) & srcMask;
            auto srcNullBitmap = getParam(src);
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

    auto i = query->projectionBegin();
    for (decltype(destRecord.fixedSizeFieldCount()) destFieldIdx = 0u; destFieldIdx < destRecord.fixedSizeFieldCount();
            ++i, ++destFieldIdx) {
        auto srcFieldIdx = *i;
        auto& srcMeta = mRecord.getFieldMeta(srcFieldIdx);
        auto& destMeta = destRecord.getFieldMeta(destFieldIdx);
        auto& field = srcMeta.field;
        LOG_ASSERT(field.isFixedSized(), "Field must be fixed size");

        auto fieldAlignment = field.alignOf();
        auto fieldPtrType = getFieldPtrTy(field.type());

        // -> auto srcData = reinterpret_cast<const T*>(src + srcMeta.offset);
        auto srcData = getParam(src);
        if (srcMeta.offset != 0) {
            srcData = CreateInBoundsGEP(srcData, getInt64(srcMeta.offset));
        }
        srcData = CreateBitCast(srcData, fieldPtrType);

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

    llvm::Value* destHeapOffset = getInt32(destRecord.staticSize());

    if (destRecord.varSizeFieldCount() != 0) {
        auto srcFieldIdx = mRecord.fixedSizeFieldCount();
        decltype(destRecord.varSizeFieldCount()) destFieldIdx = 0;

        auto& srcMeta = mRecord.getFieldMeta(srcFieldIdx);
        auto& destMeta = destRecord.getFieldMeta(destRecord.fixedSizeFieldCount());
        auto& field = srcMeta.field;
        LOG_ASSERT(!field.isFixedSized(), "Field must be variable size");

        // -> auto srcData = reinterpret_cast<uint32_t*>(src + srcMeta.offset);
        auto srcData = getParam(src);
        if (srcMeta.offset != 0) {
            srcData = CreateInBoundsGEP(srcData, getInt64(srcMeta.offset));
        }
        srcData = CreateBitCast(srcData, getInt32PtrTy());

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
                // -> srcData += (*i - srcFieldIdx);
                srcData = CreateInBoundsGEP(srcData, getInt64(step));
                srcFieldIdx = *i;
            }

            // -> auto srcHeapOffset = *srcData;
            auto srcHeapOffset = CreateAlignedLoad(srcData, 4u);

            // -> auto offsetCorrection = srcHeapOffset-+ destHeapOffset;
            auto offsetCorrection = CreateSub(srcHeapOffset, destHeapOffset);

            llvm::Value* offset;
            do {
                ++i;

                // -> ++srcData;
                ++srcFieldIdx;
                srcData = CreateInBoundsGEP(srcData, getInt64(1));

                // -> auto offset = *srcData - offsetCorrection;
                offset = CreateAlignedLoad(srcData, 4u);
                offset = CreateSub(offset, offsetCorrection);

                // -> ++destData;
                ++destFieldIdx;
                destData = CreateInBoundsGEP(destData, getInt64(1));

                // -> *destData = offset;
                CreateAlignedStore(offset, destData, 4u);
            } while (destFieldIdx < destRecord.varSizeFieldCount() && *i == srcFieldIdx);

            // -> auto srcHeap = page + srcHeapOffset;
            auto srcHeap = CreateInBoundsGEP(getParam(src), srcHeapOffset);

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

} // namespace store
} // namespace tell
