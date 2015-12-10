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
    auto mainPage = CreateBitCast(getParam(page), mMainPageStructTy->getPointerTo());

    auto index = CreateZExt(getParam(idx), getInt64Ty());

    auto count = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(0) });
    count = CreateZExt(CreateAlignedLoad(count, 4u), getInt64Ty());

    if (query->headerLength() != 0u) {
        CreateMemSet(getParam(data), getInt8(0), query->headerLength(), 8u);
    }

    auto& srcRecord = mContext.record();
    auto& destRecord = query->record();
    llvm::Value* headerData = nullptr;
    Record::id_t destFieldIdx = 0u;
    auto end = query->projectionEnd();
    for (auto i = query->projectionBegin(); i != end; ++i, ++destFieldIdx) {
        auto srcFieldIdx = *i;
        auto& srcFieldMeta = srcRecord.getFieldMeta(srcFieldIdx);
        auto& field = srcFieldMeta.field;
        if (field.isNotNull()) {
            continue;
        }
        if (!headerData) {
            // auto headerOffset = mainPage->headerOffset;
            auto headerOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(1) });
            headerOffset = CreateZExt(CreateAlignedLoad(headerOffset, 4u), getInt64Ty());

            // -> auto headerData = page + headerOffset;
            headerData = CreateInBoundsGEP(getParam(page), headerOffset);
            headerData = CreateInBoundsGEP(headerData, createConstMul(index, query->headerLength()));
        }

        auto srcIdx = srcFieldIdx / 8u;
        auto srcBitIdx = srcFieldIdx % 8u;
        uint8_t srcMask = (0x1u << srcBitIdx);

        auto srcNullBitmap = headerData;
        if (srcIdx != 0) {
            srcNullBitmap = CreateInBoundsGEP(srcNullBitmap, getInt64(srcIdx));
        }
        srcNullBitmap = CreateAnd(CreateAlignedLoad(srcNullBitmap, 1u), getInt8(srcMask));

        auto destIdx = destFieldIdx / 8u;
        auto destBitIdx = destFieldIdx % 8u;

        if (destBitIdx > srcBitIdx) {
            srcNullBitmap = CreateShl(srcNullBitmap, getInt64(destBitIdx - srcBitIdx));
        } else if (destBitIdx < srcBitIdx) {
            srcNullBitmap = CreateLShr(srcNullBitmap, getInt64(srcBitIdx - destBitIdx));
        }

        auto destNullBitmap = getParam(data);
        if (destIdx != 0) {
            destNullBitmap = CreateInBoundsGEP(destNullBitmap, getInt64(destIdx));
        }

        auto res = CreateOr(CreateAlignedLoad(destNullBitmap, 1u), srcNullBitmap);
        CreateAlignedStore(res, destNullBitmap, 1u);
    }

    llvm::Value* fixedData = nullptr;
    destFieldIdx = 0u;
    for (auto i = query->projectionBegin(); i != end; ++i, ++destFieldIdx) {
        auto srcFieldIdx = *i;
        auto& srcFieldMeta = srcRecord.getFieldMeta(srcFieldIdx);
        auto& field = srcFieldMeta.field;
        auto fieldAlignment = field.alignOf();
        auto fieldPtrType = getFieldPtrTy(field.type());

        auto& fixedMetaData = mContext.fixedMetaData();
        auto srcFieldOffset = fixedMetaData[srcFieldIdx].offset;

        auto destFieldOffset = destRecord.getFieldMeta(destFieldIdx).offset;

        LOG_ASSERT(field.isFixedSized(), "Only fixed size supported");
        if (!fixedData) {
            // auto fixedOffset = mainPage->fixedOffset;
            auto fixedOffset = CreateInBoundsGEP(mainPage, { getInt64(0), getInt32(2) });
            fixedOffset = CreateZExt(CreateAlignedLoad(fixedOffset, 4u), getInt64Ty());

            // -> auto fixedData = page + fixedOffset;
            fixedData = CreateInBoundsGEP(getParam(page), fixedOffset);
        }

        auto src = fixedData;
        if (srcFieldOffset != 0) {
            src = CreateInBoundsGEP(src, createConstMul(count, srcFieldOffset));
        }
        src = CreateBitCast(src, fieldPtrType);
        src = CreateInBoundsGEP(src, index);

        auto dest = getParam(data);
        if (destFieldOffset != 0) {
            dest = CreateInBoundsGEP(dest, getInt64(destFieldOffset));
        }
        dest = CreateBitCast(dest, fieldPtrType);
        CreateAlignedStore(CreateAlignedLoad(src, fieldAlignment), dest, fieldAlignment);
    }

    CreateRet(getInt32(destRecord.staticSize()));
}

} // namespace deltamain
} // namespace store
} // namespace tell
