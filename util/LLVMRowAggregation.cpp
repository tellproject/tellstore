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

#include "LLVMRowAggregation.hpp"

#include <util/ScanQuery.hpp>

#include <llvm/IR/Module.h>

namespace tell {
namespace store {

const std::string LLVMRowAggregationBuilder::FUNCTION_NAME = "rowMaterialize.";

LLVMRowAggregationBuilder::LLVMRowAggregationBuilder(const Record& record, llvm::Module& module,
        llvm::TargetMachine* target, uint32_t index)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()),
                createFunctionName(index)),
          mRecord(record) {
    // Set noalias hints (data pointers are not allowed to overlap)
    mFunction->setDoesNotAlias(1);
    mFunction->setOnlyReadsMemory(1);
    mFunction->setDoesNotAlias(3);
}

void LLVMRowAggregationBuilder::build(ScanQuery* query) {
    auto& destRecord = query->record();

    auto i = query->aggregationBegin();
    for (decltype(destRecord.fieldCount()) j = 0u; j < destRecord.fieldCount(); ++i, ++j) {
        uint16_t srcFieldIdx;
        AggregationType aggregationType;
        std::tie(srcFieldIdx, aggregationType) = *i;

        uint16_t destFieldIdx;
        destRecord.idOf(crossbow::to_string(j), destFieldIdx);

        auto& srcFieldMeta = mRecord.getFieldMeta(srcFieldIdx);
        auto& srcField = srcFieldMeta.field;
        auto srcFieldAlignment = srcField.alignOf();
        auto srcFieldPtrType = getFieldPtrTy(srcField.type());
        auto srcFieldOffset = srcFieldMeta.offset;

        auto& destFieldMeta = destRecord.getFieldMeta(destFieldIdx);
        auto& destField = destFieldMeta.field;
        auto destFieldAlignment = destField.alignOf();
        auto destFieldPtrType = getFieldPtrTy(destField.type());
        auto destFieldOffset = destFieldMeta.offset;
        LOG_ASSERT(srcField.isFixedSized() && destField.isFixedSized(), "Only fixed size supported");

        llvm::Value* srcNullBitmap = nullptr;
        if (!srcField.isNotNull()) {
            auto srcIdx = srcFieldIdx / 8u;
            auto srcBitIdx = srcFieldIdx % 8u;
            uint8_t srcMask = (0x1u << srcBitIdx);

            srcNullBitmap = getParam(src);
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

            auto destNullBitmap = getParam(dest);
            if (destIdx != 0) {
                destNullBitmap = CreateInBoundsGEP(destNullBitmap, getInt64(destIdx));
            }

            auto res = CreateAnd(CreateAlignedLoad(destNullBitmap, 1u), CreateNeg(srcNullBitmap));
            CreateStore(res, destNullBitmap);
        }

        auto srcData = getParam(src);
        if (srcFieldOffset != 0) {
            srcData = CreateInBoundsGEP(srcData, getInt64(srcFieldOffset));
        }
        srcData = CreateBitCast(srcData, srcFieldPtrType);
        srcData = CreateAlignedLoad(srcData, srcFieldAlignment);

        auto destData = getParam(dest);
        if (destFieldOffset != 0) {
            destData = CreateInBoundsGEP(destData, getInt64(destFieldOffset));
        }
        destData = CreateBitCast(destData, destFieldPtrType);
        llvm::Value* destValue = CreateAlignedLoad(destData, destFieldAlignment);

        if (!srcField.isNotNull()) {
            srcNullBitmap = CreateICmp(llvm::CmpInst::ICMP_EQ, srcNullBitmap, getInt8(0));
        }

        auto isFloat = (srcField.type() == FieldType::FLOAT) || (srcField.type() == FieldType::DOUBLE);

        switch (aggregationType) {
        case AggregationType::MIN: {
            auto cond = (isFloat
                    ? CreateFCmp(llvm::CmpInst::FCMP_OLT, srcData, destValue)
                    : CreateICmp(llvm::CmpInst::ICMP_SLT, srcData, destValue));
            if (!srcField.isNotNull()) {
                cond = CreateAnd(cond, srcNullBitmap);
            }
            destValue = CreateSelect(cond, srcData, destValue);
        } break;

        case AggregationType::MAX: {
            auto cond = (isFloat
                    ? CreateFCmp(llvm::CmpInst::FCMP_OGT, srcData, destValue)
                    : CreateICmp(llvm::CmpInst::ICMP_SGT, srcData, destValue));
            if (!srcField.isNotNull()) {
                cond = CreateAnd(cond, srcNullBitmap);
            }
            destValue = CreateSelect(cond, srcData, destValue);
        } break;

        case AggregationType::SUM: {
            if (srcField.type() == FieldType::SMALLINT || srcField.type() == FieldType::INT) {
                srcData = CreateSExt(srcData, getInt64Ty());
            } else if (srcField.type() == FieldType::FLOAT) {
                srcData = CreateFPExt(srcData, getDoubleTy());
            }

            auto res = (isFloat
                    ? CreateFAdd(destValue, srcData)
                    : CreateAdd(destValue, srcData));
            if (!srcField.isNotNull()) {
                destValue = CreateSelect(srcNullBitmap, res, destValue);
            } else {
                destValue = res;
            }
        } break;

        case AggregationType::CNT: {
            if (!srcField.isNotNull()) {
                destValue = CreateAdd(destValue, CreateZExt(srcNullBitmap, getInt64Ty()));
            } else {
                destValue = CreateAdd(destValue, getInt64(1));
            }
        } break;

        default: {
            LOG_ASSERT(false, "Unknown aggregation type");
            destValue = nullptr;
        } break;
        }

        CreateAlignedStore(destValue, destData, destFieldAlignment);
    }

    // -> return destRecord.staticSize();
    CreateRet(getInt32(destRecord.staticSize()));
}

} // namespace store
} // namespace tell
