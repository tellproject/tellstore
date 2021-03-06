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

LLVMRowProjectionBuilder::LLVMRowProjectionBuilder(const Record& record, llvm::Module& module,
        llvm::TargetMachine* target, const std::string& name)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()), name),
          mRecord(record) {
    // Set noalias hints (data pointers are not allowed to overlap)
    mFunction->setDoesNotAlias(1);
    mFunction->setOnlyReadsMemory(1);
    mFunction->setDoesNotAlias(3);
}

void LLVMRowProjectionBuilder::build(ScanQuery* query) {
    auto& destRecord = query->record();

    if (destRecord.headerSize() != 0u) {
        auto i = query->projectionBegin();
        for (decltype(destRecord.fieldCount()) destFieldIdx = 0u; destFieldIdx < destRecord.fieldCount();
                ++i, ++destFieldIdx) {
            auto srcFieldIdx = *i;
            auto& srcMeta = mRecord.getFieldMeta(srcFieldIdx);
            auto& destMeta = destRecord.getFieldMeta(destFieldIdx);
            auto& field = srcMeta.field;
            if (field.isNotNull()) {
                continue;
            }

            // -> auto srcData = src + srcMeta.nullIdx;
            auto srcData = getParam(src);
            if (srcMeta.nullIdx != 0) {
                srcData = CreateInBoundsGEP(srcData, getInt64(srcMeta.nullIdx));
            }

            // -> auto nullValue = *srcData;
            auto nullValue = CreateAlignedLoad(srcData, 1u);

            // -> auto destData = dest + destMeta.nullIdx;
            auto destData = getParam(dest);
            if (destMeta.nullIdx != 0) {
                destData = CreateInBoundsGEP(destData, getInt64(destMeta.nullIdx));
            }

            // -> *destNullData = nullValue;
            CreateAlignedStore(nullValue, destData, 1u);
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

        // -> auto srcData = reinterpret_cast<const uint32_t*>(src + mRecord.variableOffset());
        auto srcData = getParam(src);
        if (mRecord.variableOffset() != 0) {
            srcData = CreateInBoundsGEP(srcData, getInt64(mRecord.variableOffset()));
        }
        srcData = CreateBitCast(srcData, getInt32PtrTy());

        // -> auto destData = reinterpret_cast<uint32_t*>(dest + destRecord.variableOffset());
        auto destData = getParam(dest);
        if (destRecord.variableOffset() != 0) {
            destData = CreateInBoundsGEP(destData, getInt64(destRecord.variableOffset()));
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

            // -> auto offsetCorrection = srcHeapOffset - destHeapOffset;
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
            auto srcHeap = CreateInBoundsGEP(getParam(src), CreateZExt(srcHeapOffset, getInt64Ty()));

            // -> auto destHeap = dest + destHeapOffset;
            auto destHeap = CreateInBoundsGEP(getParam(dest), CreateZExt(destHeapOffset, getInt64Ty()));

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
