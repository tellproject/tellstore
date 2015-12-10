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
    if (query->headerLength() != 0u) {
        CreateMemSet(getParam(destData), getInt8(0), query->headerLength(), 8u);
    }

    auto& destRecord = query->record();
    Record::id_t destFieldIdx = 0u;
    auto end = query->projectionEnd();
    for (auto i = query->projectionBegin(); i != end; ++i, ++destFieldIdx) {
        auto srcFieldIdx = *i;
        auto& srcFieldMeta = mRecord.getFieldMeta(srcFieldIdx);
        auto& field = srcFieldMeta.field;

        if (!field.isNotNull()) {
            auto srcIdx = srcFieldIdx / 8u;
            auto srcBitIdx = srcFieldIdx % 8u;
            uint8_t srcMask = (0x1u << srcBitIdx);

            auto srcNullBitmap = getParam(srcData);
            if (srcIdx != 0) {
                srcNullBitmap = CreateInBoundsGEP(srcNullBitmap, getInt64(srcIdx));
            }
            srcNullBitmap = CreateAnd(CreateLoad(srcNullBitmap), getInt8(srcMask));

            auto destIdx = destFieldIdx / 8u;
            auto destBitIdx = destFieldIdx % 8u;

            if (destBitIdx > srcBitIdx) {
                srcNullBitmap = CreateShl(srcNullBitmap, getInt64(destBitIdx - srcBitIdx));
            } else if (destBitIdx < srcBitIdx) {
                srcNullBitmap = CreateLShr(srcNullBitmap, getInt64(srcBitIdx - destBitIdx));
            }

            auto destNullBitmap = getParam(destData);
            if (destIdx != 0) {
                destNullBitmap = CreateInBoundsGEP(destNullBitmap, getInt64(destIdx));
            }

            auto res = CreateOr(CreateLoad(destNullBitmap), srcNullBitmap);
            CreateStore(res, destNullBitmap);
        }

        auto srcFieldOffset = srcFieldMeta.offset;
        auto destFieldOffset = destRecord.getFieldMeta(destFieldIdx).offset;
        LOG_ASSERT(srcFieldOffset >= 0 && destFieldOffset >= 0, "Only fixed size supported at the moment");

        auto fieldAlignment = field.alignOf();
        auto fieldPtrType = getFieldPtrTy(field.type());
        auto src = getParam(srcData);
        if (srcFieldOffset != 0) {
            src = CreateInBoundsGEP(src, getInt64(srcFieldOffset));
        }
        src = CreateBitCast(src, fieldPtrType);
        auto value = CreateAlignedLoad(src, fieldAlignment);

        auto dest = getParam(destData);
        if (destFieldOffset != 0) {
            dest = CreateInBoundsGEP(dest, getInt64(destFieldOffset));
        }
        dest = CreateBitCast(dest, fieldPtrType);
        CreateAlignedStore(value, dest, fieldAlignment);
    }

    CreateRet(getInt32(destRecord.staticSize()));
}

} // namespace store
} // namespace tell
