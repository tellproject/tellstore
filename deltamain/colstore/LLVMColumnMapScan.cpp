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

#include "LLVMColumnMapScan.hpp"

#include "ColumnMapContext.hpp"
#include "LLVMColumnMapUtils.hpp"

namespace tell {
namespace store {
namespace deltamain {

const std::string LLVMColumnMapScanBuilder::FUNCTION_NAME = "columnScan";

LLVMColumnMapScanBuilder::LLVMColumnMapScanBuilder(const ColumnMapContext& context, llvm::Module& module,
        llvm::TargetMachine* target)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()),
                FUNCTION_NAME),
          mContext(context),
          mMainPageStructTy(getColumnMapMainPageTy(module.getContext())),
          mHeapEntryStructTy(getColumnMapHeapEntriesTy(module.getContext())),
          mCount(nullptr),
          mFixedData(nullptr),
          mVariableData(nullptr),
          mRegisterWidth(mTargetInfo.getRegisterBitWidth(true)) {
    // Set noalias hints (data pointers are not allowed to overlap)
    mFunction->setDoesNotAlias(1);
    mFunction->setOnlyReadsMemory(1);
    mFunction->setDoesNotAlias(2);
    mFunction->setOnlyReadsMemory(2);
    mFunction->setDoesNotAlias(3);
    mFunction->setOnlyReadsMemory(3);
    mFunction->setDoesNotAlias(4);
    mFunction->setOnlyReadsMemory(4);
    mFunction->setDoesNotAlias(7);
}

void LLVMColumnMapScanBuilder::buildScan(const ScanAST& scanAst) {
    if (scanAst.queries.empty()) {
        CreateRetVoid();
        return;
    }

    mMainPage = CreateBitCast(getParam(page), mMainPageStructTy->getPointerTo());

    mCount = CreateInBoundsGEP(mMainPage, { getInt64(0), getInt32(0) });
    mCount = CreateZExt(CreateAlignedLoad(mCount, 4u), getInt64Ty());

    // Field evaluation
    if (!scanAst.fields.empty()) {
        mScalarConjunctsGenerated.resize(scanAst.numConjunct, false);
        mVectorConjunctsGenerated.resize(scanAst.numConjunct, false);

        if (scanAst.needsNull) {
            // -> auto headerOffset = static_cast<uint64_t>(mainPage->headerOffset);
            auto headerOffset = CreateInBoundsGEP(mMainPage, { getInt64(0), getInt32(1) });
            headerOffset = CreateZExt(CreateAlignedLoad(headerOffset, 4u), getInt64Ty());

            // -> auto headerData = page + headerOffset;
            mHeaderData = CreateInBoundsGEP(getParam(page), headerOffset);
        }

        auto i = scanAst.fields.begin();

        if (i != scanAst.fields.end() && i->second.isFixedSize) {
            // auto fixedOffset = mainPage->fixedOffset;
            auto fixedOffset = CreateInBoundsGEP(mMainPage, { getInt64(0), getInt32(2) });
            fixedOffset = CreateZExt(CreateAlignedLoad(fixedOffset, 4u), getInt64Ty());

            // -> auto src = page + fixedOffset;
            mFixedData = CreateInBoundsGEP(getParam(page), fixedOffset);

            for (; i != scanAst.fields.end() && i->second.isFixedSize; ++i) {
                buildFixedField(scanAst, i->second);
            }
        }

        if (i != scanAst.fields.end()) {
            // auto variableOffset = static_cast<uint64_t>(mainPage->variableOffset);
            auto variableOffset = CreateInBoundsGEP(mMainPage, { getInt64(0), getInt32(3) });
            variableOffset = CreateZExt(CreateAlignedLoad(variableOffset, 4u), getInt64Ty());

            // -> auto variableData = reinterpret_cast<const ColumnMapHeapEntry*>(page + variableOffset);
            mVariableData = CreateInBoundsGEP(getParam(page), variableOffset);
            mVariableData = CreateBitCast(mVariableData, mHeapEntryStructTy->getPointerTo());

            for (; i != scanAst.fields.end(); ++i) {
                buildVariableField(scanAst, i->second);
            }
        }
    }

    // Query evaluation
    buildQuery(scanAst.needsKey, scanAst.queries);

    // Result evaluation
    buildResult(scanAst.queries);

    CreateRetVoid();
}

void LLVMColumnMapScanBuilder::buildFixedField(const ScanAST& scanAst, const FieldAST& fieldAst) {
    auto start = getParam(startIdx);
    auto vectorSize = mRegisterWidth / (fieldAst.size * 8);

    // Load the start pointer of the column
    llvm::Value* srcData = nullptr;
    if (fieldAst.needsValue) {
        auto& fixedMetaData = mContext.fixedMetaData();
        auto offset = fixedMetaData[fieldAst.id].offset;

        srcData = mFixedData;
        if (offset != 0) {
            srcData = CreateInBoundsGEP(srcData, createConstMul(mCount, offset));
        }
        srcData = CreateBitCast(srcData, getFieldPtrTy(fieldAst.type));
        srcData = CreateInBoundsGEP(srcData, start);
    }

    // Load the start pointer to the null bytevector
    llvm::Value* nullData = nullptr;
    if (!fieldAst.isNotNull) {
        auto startOffset = start;
        if (fieldAst.nullIdx != 0) {
            startOffset = CreateAdd(startOffset, createConstMul(mCount, fieldAst.nullIdx));
        }
        nullData = CreateInBoundsGEP(mHeaderData, startOffset);
    }

    if (vectorSize != 1) {
        // Vectorized field evaluation
        std::tie(start, srcData, nullData) = buildFixedFieldEvaluation(srcData, nullData, start, vectorSize,
                mVectorConjunctsGenerated, scanAst, fieldAst, llvm::Twine("vector"));
    }

    // Scalar field evaluation
    buildFixedFieldEvaluation(srcData, nullData, start, 1, mScalarConjunctsGenerated, scanAst, fieldAst,
            llvm::Twine("scalar"));
}

std::tuple<llvm::Value*, llvm::Value*, llvm::Value*> LLVMColumnMapScanBuilder::buildFixedFieldEvaluation(
        llvm::Value* srcData, llvm::Value* nullData, llvm::Value* start, uint64_t vectorSize,
        std::vector<uint8_t>& conjunctsGenerated, const ScanAST& scanAst, const FieldAST& fieldAst,
        const llvm::Twine& name) {
    auto end = getParam(endIdx);
    if (vectorSize != 1) {
        auto count = CreateSub(end, start);
        count = CreateAnd(count, getInt64(-vectorSize));
        end = CreateAdd(start, count);
    }

    auto previousBlock = GetInsertBlock();
    auto bodyBlock = createBasicBlock("col." + llvm::Twine(fieldAst.id) + "." + name + ".body");
    auto endBlock = llvm::BasicBlock::Create(Context, "col." + llvm::Twine(fieldAst.id) + "." + name + ".end");
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, start, end), bodyBlock, endBlock);
    SetInsertPoint(bodyBlock);

    // -> auto idx = start;
    auto idx = CreatePHI(getInt64Ty(), 2);
    idx->addIncoming(start, previousBlock);

    // -> auto lhsPhi = srcData;
    llvm::PHINode* lhsPhi = nullptr;
    if (srcData) {
        lhsPhi = CreatePHI(srcData->getType(), 2);
        lhsPhi->addIncoming(srcData, previousBlock);
    }

    // -> auto nullPhi = nullData;
    llvm::PHINode* nullPhi = nullptr;
    if (nullData) {
        nullPhi = CreatePHI(nullData->getType(), 2);
        nullPhi->addIncoming(nullData, previousBlock);
    }

    auto fieldTy = getFieldVectorTy(fieldAst.type, vectorSize);
    auto nullTy = getInt8VectorTy(vectorSize);
    auto conjunctTy = getInt8VectorTy(vectorSize);

    // -> auto lhsValue = *lhsPhi;
    llvm::Value* lhsValue = nullptr;
    if (srcData) {
        lhsValue = CreateBitCast(lhsPhi, fieldTy->getPointerTo());
        lhsValue = CreateAlignedLoad(lhsValue, fieldAst.alignment);
    }

    // -> auto nullValue = *nullPhi;
    llvm::Value* nullValue = nullptr;
    if (nullData) {
        nullValue = CreateBitCast(nullPhi, nullTy->getPointerTo());
        nullValue = CreateAlignedLoad(nullValue, 1u);
    }

    // Evaluate all predicates attached to this field
    for (auto& predicateAst : fieldAst.predicates) {
        auto& rhsAst = predicateAst.fixed;

        llvm::Value* res;
        if (predicateAst.type == PredicateType::IS_NULL) {
            // Check if the field is null
            res = nullValue;
        } else if (predicateAst.type == PredicateType::IS_NOT_NULL) {
            res = CreateXor(nullValue, getInt8Vector(vectorSize, 1));
        } else {
            LOG_ASSERT(lhsValue != nullptr, "lhs must not be null for this kind of comparison");
            // Execute the comparison
            auto rhsValue = getVector(vectorSize, rhsAst.value);
            res = (rhsAst.isFloat
                ? CreateFCmp(rhsAst.predicate, lhsValue, rhsValue)
                : CreateICmp(rhsAst.predicate, lhsValue, rhsValue));
            res = CreateZExtOrBitCast(res, conjunctTy);

            // The predicate evaluates to false if the value is null
            if (nullData) {
                res = CreateAnd(res, CreateXor(nullValue, getInt8Vector(vectorSize, 1)));
            }
        }

        // Store resulting conjunct value
        auto& conjunctProperties = scanAst.conjunctProperties[predicateAst.conjunct];
        LOG_ASSERT(conjunctProperties.predicateCount > 0, "Conjunct must have predicates");

        llvm::Value* conjunctPtr;
        if (conjunctProperties.predicateCount == 1) {
            auto queryIndex = conjunctProperties.queryIndex;
            auto& query = scanAst.queries[queryIndex];
            decltype(predicateAst.conjunct) conjunctIdx;
            if (!query.shared) {
                conjunctIdx = queryIndex;
            } else {
                for (conjunctIdx = query.conjunctOffset; conjunctIdx < predicateAst.conjunct; ++conjunctIdx) {
                    if (scanAst.conjunctProperties[conjunctIdx].predicateCount < 2) {
                        break;
                    }
                }
            }

            conjunctPtr = idx;
            if (conjunctIdx > 0) {
                conjunctPtr = CreateAdd(createConstMul(mCount, conjunctIdx), conjunctPtr);
            }
            conjunctPtr = CreateInBoundsGEP(getParam(resultData), conjunctPtr);
            conjunctPtr = CreateBitCast(conjunctPtr, conjunctTy->getPointerTo());
            if (conjunctsGenerated[conjunctIdx]) {
                res = CreateAnd(CreateAlignedLoad(conjunctPtr, 1u), res);
            } else {
                conjunctsGenerated[conjunctIdx] = true;
            }
        } else {
            conjunctPtr = CreateAdd(createConstMul(mCount, predicateAst.conjunct), idx);
            conjunctPtr = CreateInBoundsGEP(getParam(resultData), conjunctPtr);
            conjunctPtr = CreateBitCast(conjunctPtr, conjunctTy->getPointerTo());
            if (conjunctsGenerated[predicateAst.conjunct]) {
                res = CreateOr(CreateAlignedLoad(conjunctPtr, 1u), res);
            } else {
                conjunctsGenerated[predicateAst.conjunct] = true;
            }
        }
        CreateAlignedStore(res, conjunctPtr, 1u);
    }

    // -> idx += vectorSize;
    auto idxNext = CreateAdd(idx, getInt64(vectorSize));
    idx->addIncoming(idxNext, GetInsertBlock());

    // -> lhs += vectorSize;
    llvm::Value* lhsNext = nullptr;
    if (srcData) {
        lhsNext = CreateInBoundsGEP(lhsPhi, getInt64(vectorSize));
        lhsPhi->addIncoming(lhsNext, GetInsertBlock());
    }

    // -> nullValue += vectorSize;
    llvm::Value* nullNext = nullptr;
    if (nullData) {
        nullNext = CreateInBoundsGEP(nullPhi, getInt64(vectorSize));
        nullPhi->addIncoming(nullNext, GetInsertBlock());
    }

    // -> idx != end
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, idxNext, end), bodyBlock, endBlock);

    mFunction->getBasicBlockList().push_back(endBlock);
    SetInsertPoint(endBlock);

    llvm::PHINode* nullEnd = nullptr;
    llvm::PHINode* lhsEnd = nullptr;
    if (vectorSize != 1) {
        if (nullData) {
            nullEnd = CreatePHI(nullData->getType(), 2);
            nullEnd->addIncoming(nullData, previousBlock);
            nullEnd->addIncoming(nullNext, bodyBlock);
        }

        if (srcData) {
            lhsEnd = CreatePHI(srcData->getType(), 2);
            lhsEnd->addIncoming(srcData, previousBlock);
            lhsEnd->addIncoming(lhsNext, bodyBlock);
        }
    }

    return std::make_tuple(end, lhsEnd, nullEnd);
}

void LLVMColumnMapScanBuilder::buildVariableField(const ScanAST& scanAst, const FieldAST& fieldAst) {
    auto start = getParam(startIdx);
    auto end = getParam(endIdx);

    // Load the start pointer of the column
    llvm::Value* srcStartData = nullptr;
    llvm::Value* srcEndData = nullptr;
    if (fieldAst.needsValue) {
        auto& record = mContext.record();
        auto destIdx = fieldAst.id - record.fixedSizeFieldCount();

        auto startOffset = start;
        if (destIdx != 0) {
            startOffset = CreateAdd(startOffset, createConstMul(mCount, destIdx));
        }
        srcStartData = CreateInBoundsGEP(mVariableData, startOffset);

        if (fieldAst.id + 1u == record.fieldCount()) {
            srcEndData = CreateGEP(mVariableData, CreateSub(start, getInt64(1)));
        } else {
            srcEndData = CreateInBoundsGEP(srcStartData, mCount);
        }
    }

    // Load the start pointer to the null bytevector
    llvm::Value* nullData = nullptr;
    if (!fieldAst.isNotNull) {
        auto startOffset = start;
        if (fieldAst.nullIdx != 0) {
            startOffset = CreateAdd(startOffset, createConstMul(mCount, fieldAst.nullIdx));
        }
        nullData = CreateInBoundsGEP(mHeaderData, startOffset);
    }

    auto previousBlock = GetInsertBlock();
    auto bodyBlock = createBasicBlock("col." + llvm::Twine(fieldAst.id) + ".body");
    auto endBlock = llvm::BasicBlock::Create(Context, "col." + llvm::Twine(fieldAst.id) + ".end");
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, start, end), bodyBlock, endBlock);
    SetInsertPoint(bodyBlock);

    // -> auto idx = start;
    auto idx = CreatePHI(getInt64Ty(), 2);
    idx->addIncoming(start, previousBlock);

    // -> auto heapEntryStartPhi = srcStartData;
    // -> auto heapEntryEndPhi = srcEndData;
    llvm::PHINode* heapEntryStartPhi = nullptr;
    llvm::PHINode* heapEntryEndPhi = nullptr;
    if (srcStartData) {
        heapEntryStartPhi = CreatePHI(srcStartData->getType(), 2);
        heapEntryStartPhi->addIncoming(srcStartData, previousBlock);

        heapEntryEndPhi = CreatePHI(srcEndData->getType(), 2);
        heapEntryEndPhi->addIncoming(srcEndData, previousBlock);
    }

    // -> auto nullPhi = nullData;
    llvm::PHINode* nullPhi = nullptr;
    if (nullData) {
        nullPhi = CreatePHI(nullData->getType(), 2);
        nullPhi->addIncoming(nullData, previousBlock);
    }

    llvm::Value* lhsStart = nullptr;
    llvm::Value* length = nullptr;
    llvm::Value* prefix = nullptr;
    if (srcStartData) {
        // -> auto heapStartOffset = heapEntryStartPhi->offset;
        auto heapStartOffset = CreateInBoundsGEP(heapEntryStartPhi, { getInt64(0), getInt32(0) });
        heapStartOffset = CreateAlignedLoad(heapStartOffset, 4u);

        // -> auto lhsStart = page + static_cast<uint64_t>(heapStartOffset);
        lhsStart = CreateZExt(heapStartOffset, getInt64Ty());
        lhsStart = CreateInBoundsGEP(getParam(page), lhsStart);

        // -> auto prefix = heapEntryStartPhi->prefix;;
        prefix = CreateInBoundsGEP(heapEntryStartPhi, { getInt64(0), getInt32(1) });
        prefix = CreateAlignedLoad(prefix, 4u);

        // -> auto heapEndOffset = heapEntryEndPhi->offset;
        auto heapEndOffset = CreateInBoundsGEP(heapEntryEndPhi, { getInt64(0), getInt32(0) });
        heapEndOffset = CreateAlignedLoad(heapEndOffset, 4u);

        // -> auto length = heapEndOffset - heapStartOffset;
        length = CreateSub(heapEndOffset, heapStartOffset);
    }

    // -> auto nullValue = *nullPhi;
    llvm::Value* nullValue = nullptr;
    if (nullData) {
        nullValue = CreateAlignedLoad(nullPhi, 1u);
    }

    // Evaluate all predicates attached to this field
    for (decltype(fieldAst.predicates.size()) i = 0; i < fieldAst.predicates.size(); ++i) {
        auto& predicateAst = fieldAst.predicates[i];
        auto& rhsAst = predicateAst.variable;

        // Execute the comparison
        llvm::Value* res = nullptr;
        switch (predicateAst.type) {

        case PredicateType::IS_NULL: {
            res = nullValue;
        } break;

        case PredicateType::IS_NOT_NULL: {
            res = CreateXor(nullValue, getInt8(1));
        } break;

        case PredicateType::EQUAL:
        case PredicateType::NOT_EQUAL: {
            LOG_ASSERT(srcStartData != nullptr, "lhs must not be null for this kind of comparison");

            auto negateResult = (predicateAst.type == PredicateType::NOT_EQUAL);
            if (rhsAst.size == 0) {
                res = CreateICmp((negateResult ? llvm::CmpInst::ICMP_NE : llvm::CmpInst::ICMP_EQ), length, getInt32(0));
            } else {
                auto lengthComp = CreateICmp(llvm::CmpInst::ICMP_EQ, length, getInt32(rhsAst.size));
                auto prefixComp = CreateICmp(llvm::CmpInst::ICMP_EQ, prefix, getInt32(rhsAst.prefix));
                res = CreateAnd(lengthComp, prefixComp);

                if (rhsAst.size > 4) {
                    auto dataStart = CreateInBoundsGEP(lhsStart, getInt64(4));
                    auto rhsStart = CreateInBoundsGEP(rhsAst.value->getValueType(), rhsAst.value,
                            { getInt64(0), getInt32(4) });
                    auto rhsEnd = CreateGEP(rhsAst.value->getValueType(), rhsAst.value,
                            { getInt64(1), getInt32(0) });

                    res = createMemCmp(res, dataStart, rhsStart, rhsEnd,
                            "col." + llvm::Twine(fieldAst.id) + "." + llvm::Twine(i));
                }
                if (negateResult) {
                    res = CreateNot(res);
                }
            }
            res = CreateZExtOrBitCast(res, getInt8Ty());

            // The predicate evaluates to false if the value is null
            if (nullData) {
                res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
            }
        } break;

        case PredicateType::PREFIX_LIKE:
        case PredicateType::PREFIX_NOT_LIKE: {
            LOG_ASSERT(srcStartData != nullptr, "lhs must not be null for this kind of comparison");

            auto negateResult = (predicateAst.type == PredicateType::PREFIX_NOT_LIKE);
            if (rhsAst.size == 0) {
                res = (negateResult ? getFalse() : getTrue());
            } else {
                auto lengthComp = CreateICmp(llvm::CmpInst::ICMP_UGE, length, getInt32(rhsAst.size));
                auto maskedPrefix = prefix;
                if (rhsAst.size < 4) {
                    uint32_t mask = 0;
                    memset(&mask, 0xFFu, rhsAst.size);
                    maskedPrefix = CreateAnd(maskedPrefix, getInt32(mask));
                }
                auto prefixComp = CreateICmp(llvm::CmpInst::ICMP_EQ, maskedPrefix, getInt32(rhsAst.prefix));
                res = CreateAnd(lengthComp, prefixComp);

                if (rhsAst.size > 4) {
                    auto dataStart = CreateInBoundsGEP(lhsStart, getInt64(4));
                    auto rhsStart = CreateInBoundsGEP(rhsAst.value->getValueType(), rhsAst.value,
                            { getInt64(0), getInt32(4) });
                    auto rhsEnd = CreateGEP(rhsAst.value->getValueType(), rhsAst.value,
                            { getInt64(1), getInt32(0) });

                    res = createMemCmp(res, dataStart, rhsStart, rhsEnd,
                            "col." + llvm::Twine(fieldAst.id) + "." + llvm::Twine(i));
                }
                if (negateResult) {
                    res = CreateNot(res);
                }
            }
            res = CreateZExtOrBitCast(res, getInt8Ty());

            // The predicate evaluates to false if the value is null
            if (nullData) {
                res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
            }
        } break;

        case PredicateType::POSTFIX_LIKE:
        case PredicateType::POSTFIX_NOT_LIKE: {
            LOG_ASSERT(srcStartData != nullptr, "lhs must not be null for this kind of comparison");

            auto negateResult = (predicateAst.type == PredicateType::POSTFIX_NOT_LIKE);
            if (rhsAst.size == 0) {
                res = (negateResult ? getFalse() : getTrue());
            } else {
                res = createPostfixMemCmp(lhsStart, length, rhsAst.value, rhsAst.size,
                        "col." + llvm::Twine(fieldAst.id) + "." + llvm::Twine(i));
                if (negateResult) {
                    res = CreateNot(res);
                }
            }
            res = CreateZExtOrBitCast(res, getInt8Ty());

            // The predicate evaluates to false if the value is null
            if (nullData) {
                res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
            }
        } break;

        default: {
            LOG_ASSERT(false, "Unknown predicate");
            res = nullptr;
        } break;
        }

        // Store resulting conjunct value
        auto& conjunctProperties = scanAst.conjunctProperties[predicateAst.conjunct];
        LOG_ASSERT(conjunctProperties.predicateCount > 0, "Conjunct must have predicates");

        llvm::Value* conjunctPtr;
        if (conjunctProperties.predicateCount == 1) {
            auto queryIndex = conjunctProperties.queryIndex;
            auto& query = scanAst.queries[queryIndex];
            decltype(predicateAst.conjunct) conjunctIdx;
            if (!query.shared) {
                conjunctIdx = queryIndex;
            } else {
                for (conjunctIdx = query.conjunctOffset; conjunctIdx < predicateAst.conjunct; ++conjunctIdx) {
                    if (scanAst.conjunctProperties[conjunctIdx].predicateCount < 2) {
                        break;
                    }
                }
            }

            conjunctPtr = idx;
            if (conjunctIdx > 0) {
                conjunctPtr = CreateAdd(createConstMul(mCount, conjunctIdx), conjunctPtr);
            }
            conjunctPtr = CreateInBoundsGEP(getParam(resultData), conjunctPtr);
            if (mScalarConjunctsGenerated[conjunctIdx]) {
                res = CreateAnd(CreateAlignedLoad(conjunctPtr, 1u), res);
            } else {
                mScalarConjunctsGenerated[conjunctIdx] = true;
            }
        } else {
            conjunctPtr = CreateAdd(createConstMul(mCount, predicateAst.conjunct), idx);
            conjunctPtr = CreateInBoundsGEP(getParam(resultData), conjunctPtr);
            if (mScalarConjunctsGenerated[predicateAst.conjunct]) {
                res = CreateOr(CreateAlignedLoad(conjunctPtr, 1u), res);
            } else {
                mScalarConjunctsGenerated[predicateAst.conjunct] = true;
            }
        }
        CreateAlignedStore(res, conjunctPtr, 1u);
    }

    // -> idx += 1;
    auto idxNext = CreateAdd(idx, getInt64(1));
    idx->addIncoming(idxNext, GetInsertBlock());

    if (srcStartData) {
        // -> heapEntryStartPhi += 1;
        auto heapEntryStartNext = CreateInBoundsGEP(heapEntryStartPhi, getInt64(1));
        heapEntryStartPhi->addIncoming(heapEntryStartNext, GetInsertBlock());

        // -> heapEntryEndPhi += 1;
        auto heapEntryEndNext = CreateInBoundsGEP(heapEntryEndPhi, getInt64(1));
        heapEntryEndPhi->addIncoming(heapEntryEndNext, GetInsertBlock());
    }

    // -> nullPhi += 1;
    if (nullData) {
        auto nullNext = CreateInBoundsGEP(nullPhi, getInt64(1));
        nullPhi->addIncoming(nullNext, GetInsertBlock());
    }

    // -> idx != end
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, idxNext, end), bodyBlock, endBlock);

    mFunction->getBasicBlockList().push_back(endBlock);
    SetInsertPoint(endBlock);
}

void LLVMColumnMapScanBuilder::buildQuery(bool needsKey, const std::vector<QueryAST>& queries) {
    auto start = getParam(startIdx);
    auto validFromStart = CreateInBoundsGEP(getParam(validFromData), start);
    auto validToStart = CreateInBoundsGEP(getParam(validToData), start);
    llvm::Value* keyStart = nullptr;
    if (needsKey) {
        keyStart = CreateInBoundsGEP(getParam(keyData), start);
    }
    auto vectorSize = mRegisterWidth / (sizeof(uint64_t) * 8);

    // Vectorized query evaluation
    std::tie(start, validFromStart, validToStart, keyStart) = buildQueryEvaluation(start, validFromStart, validToStart,
            keyStart, vectorSize, queries, llvm::Twine("vector"));

    // Scalar query evaluation
    buildQueryEvaluation(start, validFromStart, validToStart, keyStart, 1, queries, llvm::Twine("scalar"));
}

std::tuple<llvm::Value*, llvm::Value*, llvm::Value*, llvm::Value*> LLVMColumnMapScanBuilder::buildQueryEvaluation(
        llvm::Value* start, llvm::Value* validFromStart, llvm::Value* validToStart, llvm::Value* keyStart,
        uint64_t vectorSize, const std::vector<QueryAST>& queries, const llvm::Twine& name) {
    auto end = getParam(endIdx);
    if (vectorSize != 1) {
        auto count = CreateSub(end, start);
        count = CreateAnd(count, getInt64(-vectorSize));
        end = CreateAdd(start, count);
    }

    auto previousBlock = GetInsertBlock();
    auto bodyBlock = createBasicBlock("check." + name + ".body");
    auto endBlock = llvm::BasicBlock::Create(Context, "check." + name + ".end");
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, start, end), bodyBlock, endBlock);
    SetInsertPoint(bodyBlock);

    // -> auto idx = start;
    auto idx = CreatePHI(getInt64Ty(), 2);
    idx->addIncoming(start, previousBlock);

    // -> validFromPhi = validFromStart;
    auto validFromPhi = CreatePHI(validFromStart->getType(), 2);
    validFromPhi->addIncoming(validFromStart, previousBlock);

    // -> validToPhi = validToStart;
    auto validToPhi = CreatePHI(validToStart->getType(), 2);
    validToPhi->addIncoming(validToStart, previousBlock);

    // -> keyPhi = keyStart;
    llvm::PHINode* keyPhi = nullptr;
    if (keyStart) {
        keyPhi = CreatePHI(keyStart->getType(), 2);
        keyPhi->addIncoming(keyStart, previousBlock);
    }

    auto validFromTy = getInt64VectorTy(vectorSize);
    auto validToTy = getInt64VectorTy(vectorSize);
    auto keyTy = getInt64VectorTy(vectorSize);
    auto conjunctTy = getInt8VectorTy(vectorSize);

    // Load the valid-from value of the record
    auto validFrom = CreateBitCast(validFromPhi, validFromTy->getPointerTo());
    validFrom = CreateAlignedLoad(validFrom, 8u);

    // Load the valid-to value of the record
    auto validTo = CreateBitCast(validToPhi, validToTy->getPointerTo());
    validTo = CreateAlignedLoad(validTo, 8u);

    // Load the key value of the record
    llvm::Value* key = nullptr;
    if (keyStart) {
        key = CreateBitCast(keyPhi, keyTy->getPointerTo());
        key = CreateAlignedLoad(key, 8u);
    }

    // Evaluate all queries
    for (decltype(queries.size()) i = 0; i < queries.size(); ++i) {
        auto& query = queries[i];

        // Evaluate validFrom <= version && validTo > baseVersion
        auto validFromRes = CreateICmp(llvm::CmpInst::ICMP_ULE, validFrom, getInt64Vector(vectorSize, query.version));
        auto validToRes = CreateICmp(llvm::CmpInst::ICMP_UGT, validTo, getInt64Vector(vectorSize, query.baseVersion));
        auto res = CreateAnd(validFromRes, validToRes);

        // Evaluate (key >> partitionShift) % partitionModulo == partitionNumber
        if (query.partitionModulo != 0u) {
            LOG_ASSERT(keyStart != nullptr, "No partitioning in AST");
            auto keyValue = key;
            if (query.partitionShift != 0) {
                keyValue = CreateLShr(keyValue, getInt64Vector(vectorSize, query.partitionShift));
            }
            auto keyRes = CreateICmp(llvm::CmpInst::ICMP_EQ,
                    createConstMod(keyValue, query.partitionModulo, vectorSize),
                    getInt64Vector(vectorSize, query.partitionNumber));
            res = CreateAnd(res, keyRes);
        }
        res = CreateZExtOrBitCast(res, conjunctTy);

        // Store temporary result value
        llvm::Value* resultPtr = idx;
        if (i > 0) {
            resultPtr = CreateAdd(createConstMul(mCount, i), resultPtr);
        }
        resultPtr = CreateInBoundsGEP(getParam(resultData), resultPtr);
        resultPtr = CreateBitCast(resultPtr, conjunctTy->getPointerTo());
        if (mScalarConjunctsGenerated[i]) {
            res = CreateAnd(CreateAlignedLoad(resultPtr, 1u), res);
        }
        CreateAlignedStore(res, resultPtr, 1u);
    }

    // -> idx += vectorSize;
    auto idxNext = CreateAdd(idx, getInt64(vectorSize));
    idx->addIncoming(idxNext, GetInsertBlock());

    // -> validFromPhi += vectorSize;
    auto validFromNext = CreateInBoundsGEP(validFromPhi, getInt64(vectorSize));
    validFromPhi->addIncoming(validFromNext, GetInsertBlock());

    // -> validToPhi += vectorSize;
    auto validToNext = CreateInBoundsGEP(validToPhi, getInt64(vectorSize));
    validToPhi->addIncoming(validToNext, GetInsertBlock());

    // -> keyPhi += vectorSize;
    llvm::Value* keyNext = nullptr;
    if (keyStart) {
        keyNext = CreateInBoundsGEP(keyPhi, getInt64(vectorSize));
        keyPhi->addIncoming(keyNext, GetInsertBlock());
    }

    // -> idx != end
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, idxNext, end), bodyBlock, endBlock);

    mFunction->getBasicBlockList().push_back(endBlock);
    SetInsertPoint(endBlock);

    llvm::PHINode* validFromEnd = nullptr;
    llvm::PHINode* validToEnd = nullptr;
    llvm::PHINode* keyEnd = nullptr;

    if (vectorSize != 1) {
        validFromEnd = CreatePHI(validFromStart->getType(), 2);
        validFromEnd->addIncoming(validFromStart, previousBlock);
        validFromEnd->addIncoming(validFromNext, bodyBlock);

        validToEnd = CreatePHI(validToStart->getType(), 2);
        validToEnd->addIncoming(validToStart, previousBlock);
        validToEnd->addIncoming(validToNext, bodyBlock);

        if (keyStart) {
            keyEnd = CreatePHI(keyStart->getType(), 2);
            keyEnd->addIncoming(keyStart, previousBlock);
            keyEnd->addIncoming(keyNext, bodyBlock);
        }
    }

    return std::make_tuple(end, validFromEnd, validToEnd, keyEnd);
}

void LLVMColumnMapScanBuilder::buildResult(const std::vector<QueryAST>& queries) {
    uint32_t mergedOffset = queries.size();
    auto vectorSize = mRegisterWidth / 8;
    for (decltype(queries.size()) i = 0; i < queries.size(); ++i) {
        auto& query = queries.at(i);
        if (query.numConjunct == 0u) {
            continue;
        }

        LOG_ASSERT(query.conjunctOffset <= mergedOffset, "Query offsets must be in ascending order");

        // Merge all conjunct of the query into the final or first conjunct if the conjuncts were not already merged
        auto mergeConjunct = (query.shared ? query.conjunctOffset : i);
        if (mergedOffset == query.conjunctOffset) {
            decltype(query.numConjunct) startOffset = (query.shared ? 1 : 0);
            for (auto j = startOffset; j < query.numConjunct; ++j) {
                auto src = query.conjunctOffset + j;
                if (!mScalarConjunctsGenerated[src]) {
                    continue;
                }

                buildConjunctMerge(vectorSize, src, mergeConjunct);
            }
            mergedOffset += query.numConjunct;
        }

        // Merge last conjunct of the query into the final result conjunct
        if (query.shared) {
            buildConjunctMerge(vectorSize, mergeConjunct, i);
        }
    }
}

void LLVMColumnMapScanBuilder::buildConjunctMerge(uint64_t vectorSize, uint32_t src, uint32_t dest) {
    auto start = getParam(startIdx);

    auto lhsBase = getParam(resultData);
    if (dest > 0) {
        lhsBase = CreateInBoundsGEP(lhsBase, createConstMul(mCount, dest));
    }
    auto lhsStart = CreateInBoundsGEP(lhsBase, start);

    auto rhsStart = getParam(resultData);
    if (src > 0) {
        rhsStart = CreateInBoundsGEP(rhsStart, createConstMul(mCount, src));
    }
    rhsStart = CreateInBoundsGEP(rhsStart, start);

    if (vectorSize != 1) {
        auto count = CreateSub(getParam(endIdx), start);
        count = CreateAnd(count, getInt64(-vectorSize));

        auto lhsEnd = CreateAdd(start, count);
        lhsEnd = CreateInBoundsGEP(lhsBase, lhsEnd);

        rhsStart = buildConjunctMerge(lhsStart, lhsEnd, rhsStart, vectorSize, "conj." + llvm::Twine(src) + ".vector");
        lhsStart = lhsEnd;
    }
    auto lhsEnd = CreateInBoundsGEP(lhsBase, getParam(endIdx));
    buildConjunctMerge(lhsStart, lhsEnd, rhsStart, 1, "conj." + llvm::Twine(src) + ".scalar");
}

llvm::Value* LLVMColumnMapScanBuilder::buildConjunctMerge(llvm::Value* lhsStart, llvm::Value* lhsEnd,
        llvm::Value* rhsStart, uint64_t vectorSize, const llvm::Twine& name) {
    auto previousBlock = GetInsertBlock();
    auto bodyBlock = createBasicBlock(name + ".body");
    auto endBlock = llvm::BasicBlock::Create(Context, name + ".end");
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, lhsStart, lhsEnd), bodyBlock, endBlock);
    SetInsertPoint(bodyBlock);

    // -> auto lhsPhi = lhsStart;
    auto lhsPhi = CreatePHI(lhsStart->getType(), 2);
    lhsPhi->addIncoming(lhsStart, previousBlock);

    // -> auto rhsPhi = rhsStart;
    auto rhsPhi = CreatePHI(rhsStart->getType(), 2);
    rhsPhi->addIncoming(rhsStart, previousBlock);

    auto conjunctTy = getInt8VectorTy(vectorSize);

    // Load destination conjunct
    auto lhsPtr = CreateBitCast(lhsPhi, conjunctTy->getPointerTo());
    auto lhs = CreateAlignedLoad(lhsPtr, 1u);

    // Load source conjunct
    auto rhsPtr = CreateBitCast(rhsPhi, conjunctTy->getPointerTo());
    auto rhs = CreateAlignedLoad(rhsPtr, 1u);

    // Store merged conjuncts into destination conjunct
    auto res = CreateAnd(lhs, rhs);
    CreateAlignedStore(res, lhsPtr, 1u);

    // -> lhsPhi += vectorSize;
    auto lhsNext = CreateInBoundsGEP(lhsPhi, getInt64(vectorSize));
    lhsPhi->addIncoming(lhsNext, GetInsertBlock());

    // -> rhsPhi += vectorSize;
    auto rhsNext = CreateInBoundsGEP(rhsPhi, getInt64(vectorSize));
    rhsPhi->addIncoming(rhsNext, GetInsertBlock());

    // -> lhsPhi != lhsEnd
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, lhsNext, lhsEnd), bodyBlock, endBlock);

    mFunction->getBasicBlockList().push_back(endBlock);
    SetInsertPoint(endBlock);

    llvm::PHINode* rhsEnd = nullptr;
    if (vectorSize != 1) {
        rhsEnd = CreatePHI(rhsStart->getType(), 2);
        rhsEnd->addIncoming(rhsStart, previousBlock);
        rhsEnd->addIncoming(rhsNext, bodyBlock);
    }

    return rhsEnd;
}

} // namespace deltamain
} // namespace store
} // namespace tell
