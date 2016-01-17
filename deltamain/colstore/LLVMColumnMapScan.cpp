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
                buildFixedField(i->second);
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
                buildVariableField(i->second);
            }
        }

        mScalarConjunctsGenerated.clear();
        mVectorConjunctsGenerated.clear();
    }

    // Query evaluation
    buildQuery(scanAst.needsKey, scanAst.queries);

    // Result evaluation
    buildResult(scanAst.queries);

    CreateRetVoid();
}

void LLVMColumnMapScanBuilder::buildFixedField(const FieldAST& fieldAst) {
    // Load the start pointer to the null bytevector
    llvm::Value* nullData = nullptr;
    if (!fieldAst.isNotNull) {
        nullData = mHeaderData;
        if (fieldAst.nullIdx != 0) {
            nullData = CreateInBoundsGEP(nullData, createConstMul(mCount, fieldAst.nullIdx));
        }
    }

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
    }

    // Vectorized field evaluation
    auto vectorSize = mRegisterWidth / (fieldAst.size * 8);
    auto vectorEndIdx = buildFixedFieldEvaluation(srcData, nullData, getParam(startIdx), vectorSize,
            mVectorConjunctsGenerated, fieldAst, llvm::Twine("vector"));

    // Scalar field evaluation
    buildFixedFieldEvaluation(srcData, nullData, vectorEndIdx, 1, mScalarConjunctsGenerated, fieldAst,
            llvm::Twine("scalar"));
}

llvm::Value* LLVMColumnMapScanBuilder::buildFixedFieldEvaluation(llvm::Value* srcData, llvm::Value* nullData,
        llvm::Value* startIdx, uint64_t vectorSize, std::vector<uint8_t>& conjunctsGenerated, const FieldAST& fieldAst,
        const llvm::Twine& name) {
    return createLoop(startIdx, getParam(endIdx), vectorSize, "col." + llvm::Twine(fieldAst.id) + "." + name,
            [this, srcData, nullData, vectorSize, &conjunctsGenerated, &fieldAst] (llvm::Value* idx) {
        auto nullTy = getInt8VectorTy(vectorSize);
        auto fieldTy = getFieldVectorTy(fieldAst.type, vectorSize);
        auto conjunctTy = getInt8VectorTy(vectorSize);

        // Load the null status of the value if it can be null
        llvm::Value* nullValue = nullData;
        if (nullValue) {
            nullValue = CreateInBoundsGEP(nullValue, idx);
            nullValue = CreateBitCast(nullValue, nullTy->getPointerTo());
            nullValue = CreateAlignedLoad(nullValue, 1u);
        }

        // Load the field value from the record
        llvm::Value* lhs = srcData;
        if (lhs) {
            lhs = CreateInBoundsGEP(lhs, idx);
            lhs = CreateBitCast(lhs, fieldTy->getPointerTo());
            lhs = CreateAlignedLoad(lhs, fieldAst.alignment);
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
                LOG_ASSERT(lhs != nullptr, "lhs must not be null for this kind of comparison");
                // Execute the comparison
                auto rhs = getVector(vectorSize, rhsAst.value);
                res = (rhsAst.isFloat
                    ? CreateFCmp(rhsAst.predicate, lhs, rhs)
                    : CreateICmp(rhsAst.predicate, lhs, rhs));
                res = CreateZExtOrBitCast(res, conjunctTy);

                // The predicate evaluates to false if the value is null
                if (nullValue) {
                    res = CreateAnd(res, CreateXor(nullValue, getInt8Vector(vectorSize, 1)));
                }
            }

            // Store resulting conjunct value
            auto conjunctPtr = CreateAdd(createConstMul(mCount, predicateAst.conjunct), idx);
            conjunctPtr = CreateInBoundsGEP(getParam(resultData), conjunctPtr);
            conjunctPtr = CreateBitCast(conjunctPtr, conjunctTy->getPointerTo());
            if (conjunctsGenerated[predicateAst.conjunct]) {
                res = CreateOr(CreateAlignedLoad(conjunctPtr, 1u), res);
            } else {
                conjunctsGenerated[predicateAst.conjunct] = true;
            }
            CreateAlignedStore(res, conjunctPtr, 1u);
        }
    });
}

void LLVMColumnMapScanBuilder::buildVariableField(const FieldAST& fieldAst) {
    // Load the start pointer to the null bytevector
    llvm::Value* nullData = nullptr;
    if (!fieldAst.isNotNull) {
        nullData = mHeaderData;
        if (fieldAst.nullIdx != 0) {
            nullData = CreateInBoundsGEP(nullData, createConstMul(mCount, fieldAst.nullIdx));
        }
    }

    // Load the start pointer of the column
    llvm::Value* srcData = nullptr;
    if (fieldAst.needsValue) {
        auto& record = mContext.record();
        auto destIdx = fieldAst.id - record.fixedSizeFieldCount();

        srcData = mVariableData;
        if (destIdx != 0) {
            srcData = CreateInBoundsGEP(srcData, createConstMul(mCount, destIdx));
        }
    }

    createLoop(getParam(startIdx), getParam(endIdx), 1, "col." + llvm::Twine(fieldAst.id),
            [this, srcData, nullData, &fieldAst] (llvm::Value* idx) {
        // Load the null status of the value if it can be null
        llvm::Value* nullValue = nullData;
        if (nullValue) {
            nullValue = CreateInBoundsGEP(nullValue, idx);
            nullValue = CreateAlignedLoad(nullValue, 1u);
        }

        // Load the field value from the record
        llvm::Value* lhsStart = nullptr;
        llvm::Value* length = nullptr;
        llvm::Value* prefix = nullptr;
        if (srcData) {
            auto heapEntry = CreateInBoundsGEP(srcData, idx);

            auto heapStartOffset = CreateInBoundsGEP(heapEntry, { getInt64(0), getInt32(0) });
            heapStartOffset = CreateAlignedLoad(heapStartOffset, 4u);

            lhsStart = CreateZExt(heapStartOffset, getInt64Ty());
            lhsStart = CreateInBoundsGEP(getParam(page), lhsStart);

            prefix = CreateInBoundsGEP(heapEntry, { getInt64(0), getInt32(1) });
            prefix = CreateAlignedLoad(prefix, 4u);

            auto& record = mContext.record();
            if (fieldAst.id + 1u == record.fieldCount()) {
                heapEntry = CreateGEP(mVariableData, CreateSub(idx, getInt64(1)));
            } else {
                heapEntry = CreateInBoundsGEP(heapEntry, mCount);
            }
            auto heapEndOffset = CreateInBoundsGEP(heapEntry, { getInt64(0), getInt32(0) });
            heapEndOffset = CreateAlignedLoad(heapEndOffset, 4u);

            length = CreateSub(heapEndOffset, heapStartOffset);
        }

        // Evaluate all predicates attached to this field
        for (decltype(fieldAst.predicates.size()) i = 0; i < fieldAst.predicates.size(); ++i) {
            LOG_ASSERT(srcData != nullptr, "lhs must not be null for this kind of comparison");
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
                auto negateResult = (predicateAst.type == PredicateType::NOT_EQUAL);
                if (rhsAst.size == 0) {
                    res = CreateICmp((negateResult ? llvm::CmpInst::ICMP_NE : llvm::CmpInst::ICMP_EQ), length,
                            getInt32(0));
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
                if (nullValue) {
                    res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
                }
            } break;

            case PredicateType::PREFIX_LIKE:
            case PredicateType::PREFIX_NOT_LIKE: {
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
                if (nullValue) {
                    res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
                }
            } break;

            case PredicateType::POSTFIX_LIKE:
            case PredicateType::POSTFIX_NOT_LIKE: {
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
                if (nullValue) {
                    res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
                }
            } break;

            default: {
                LOG_ASSERT(false, "Unknown predicate");
                res = nullptr;
            } break;
            }

            // Store resulting conjunct value
            auto conjunctPtr = CreateAdd(createConstMul(mCount, predicateAst.conjunct), idx);
            conjunctPtr = CreateInBoundsGEP(getParam(resultData), conjunctPtr);
            conjunctPtr = CreateBitCast(conjunctPtr, getInt8PtrTy());
            if (mScalarConjunctsGenerated[predicateAst.conjunct]) {
                res = CreateOr(CreateAlignedLoad(conjunctPtr, 1u), res);
            } else {
                mScalarConjunctsGenerated[predicateAst.conjunct] = true;
            }
            CreateAlignedStore(res, conjunctPtr, 1u);
        }
    });
}

void LLVMColumnMapScanBuilder::buildQuery(bool needsKey, const std::vector<QueryAST>& queries) {
    // Vectorized query evaluation
    auto vectorSize = mRegisterWidth / (sizeof(uint64_t) * 8);
    auto vectorEndIdx = buildQueryEvaluation(getParam(startIdx), vectorSize, needsKey, queries, llvm::Twine("vector"));

    // Scalar query evaluation
    buildQueryEvaluation(vectorEndIdx, 1, needsKey, queries, llvm::Twine("scalar"));
}

llvm::Value* LLVMColumnMapScanBuilder::buildQueryEvaluation(llvm::Value* startIdx, uint64_t vectorSize, bool needsKey,
        const std::vector<QueryAST>& queries, const llvm::Twine& name) {
    return createLoop(startIdx, getParam(endIdx), vectorSize, "check." + name,
            [this, vectorSize, needsKey, &queries] (llvm::Value* idx) {
        auto validFromTy = getInt64VectorTy(vectorSize);
        auto validToTy = getInt64VectorTy(vectorSize);
        auto keyTy = getInt64VectorTy(vectorSize);
        auto conjunctTy = getInt8VectorTy(vectorSize);

        // Load the valid-from value of the record
        auto validFrom = CreateInBoundsGEP(getParam(validFromData), idx);
        validFrom = CreateBitCast(validFrom, validFromTy->getPointerTo());
        validFrom = CreateAlignedLoad(validFrom, 8u);

        // Load the valid-to value of the record
        auto validTo = CreateInBoundsGEP(getParam(validToData), idx);
        validTo = CreateBitCast(validTo, validToTy->getPointerTo());
        validTo = CreateAlignedLoad(validTo, 8u);

        // Load the key value of the record
        llvm::Value* key = nullptr;
        if (needsKey) {
            key = CreateInBoundsGEP(getParam(keyData), idx);
            key = CreateBitCast(key, keyTy->getPointerTo());
            key = CreateAlignedLoad(key, 8u);
        }

        // Evaluate all queries
        for (decltype(queries.size()) i = 0; i < queries.size(); ++i) {
            auto& query = queries[i];

            // Evaluate validFrom <= version && validTo > baseVersion
            auto validFromRes = CreateICmp(llvm::CmpInst::ICMP_ULE, validFrom,
                    getInt64Vector(vectorSize, query.version));
            auto validToRes = CreateICmp(llvm::CmpInst::ICMP_UGT, validTo,
                    getInt64Vector(vectorSize, query.baseVersion));
            auto res = CreateAnd(validFromRes, validToRes);

            // Evaluate (key >> partitionShift) % partitionModulo == partitionNumber
            if (query.partitionModulo != 0u) {
                LOG_ASSERT(needsKey, "No partitioning in AST");
                auto keyValue = key;
                if (query.partitionShift != 0) {
                    keyValue = CreateLShr(keyValue, getInt32Vector(vectorSize, query.partitionShift));
                }
                auto keyRes = CreateICmp(llvm::CmpInst::ICMP_EQ,
                        createConstMod(keyValue, query.partitionModulo, vectorSize),
                        getInt64Vector(vectorSize, query.partitionNumber));
                res = CreateAnd(res, keyRes);
            }
            res = CreateZExtOrBitCast(res, conjunctTy);

            // Store temporary result value
            auto resultPtr = idx;
            if (i > 0) {
                resultPtr = CreateAdd(createConstMul(mCount, i), resultPtr);
            }
            resultPtr = CreateInBoundsGEP(getParam(resultData), resultPtr);
            resultPtr = CreateBitCast(resultPtr, conjunctTy->getPointerTo());
            CreateAlignedStore(res, resultPtr, 1u);
        }
    });
}

void LLVMColumnMapScanBuilder::buildResult(const std::vector<QueryAST>& queries) {
    auto vectorSize = mRegisterWidth / 8;
    for (decltype(queries.size()) i = 0; i < queries.size(); ++i) {
        auto& query = queries.at(i);
        if (query.numConjunct == 0u) {
            continue;
        }

        // Merge all conjunct of the query into the first conjunct
        for (decltype(query.numConjunct) j = query.numConjunct - 1u; j > 0u; --j) {
            auto src = query.conjunctOffset + j;
            auto dest = src - 1;

            // Vectorized conjunct merge
            auto vectorEndIdx = buildConjunctMerge(getParam(startIdx), vectorSize, src, dest, llvm::Twine("vector"));

            // Scalar conjunct merge
            buildConjunctMerge(vectorEndIdx, 1, src, dest, llvm::Twine("scalar"));
        }

        // Merge last conjunct of the query into the final result conjunct
        // Vectorized conjunct merge
        auto vectorEndIdx = buildConjunctMerge(getParam(startIdx), vectorSize, query.conjunctOffset, i,
                llvm::Twine("vector"));

        // Scalar conjunct merge
        buildConjunctMerge(vectorEndIdx, 1, query.conjunctOffset, i, llvm::Twine("scalar"));
    }
}

llvm::Value* LLVMColumnMapScanBuilder::buildConjunctMerge(llvm::Value* startIdx, uint64_t vectorSize, uint32_t src,
        uint32_t dest, const llvm::Twine& name) {
    return createLoop(startIdx, getParam(endIdx), vectorSize, "conj." + llvm::Twine(dest) + "." + name,
            [this, src, dest, vectorSize] (llvm::Value* idx) {
        auto conjunctTy = getInt8VectorTy(vectorSize);

        // Load destination conjunct
        auto lhsPtr = idx;
        if (dest > 0) {
            lhsPtr = CreateAdd(createConstMul(mCount, dest), lhsPtr);
        }
        lhsPtr = CreateInBoundsGEP(getParam(resultData), lhsPtr);
        lhsPtr = CreateBitCast(lhsPtr, conjunctTy->getPointerTo());
        auto lhs = CreateAlignedLoad(lhsPtr, 1u);

        // Load source conjunct
        auto rhsPtr = idx;
        if (src > 0) {
            rhsPtr = CreateAdd(createConstMul(mCount, src), rhsPtr);
        }
        rhsPtr = CreateInBoundsGEP(getParam(resultData), rhsPtr);
        rhsPtr = CreateBitCast(rhsPtr, conjunctTy->getPointerTo());
        auto rhs = CreateAlignedLoad(rhsPtr, 1u);

        // Store merged conjuncts into destination conjunct
        auto res = CreateAnd(lhs, rhs);
        CreateAlignedStore(res, lhsPtr, 1u);
    });
}

} // namespace deltamain
} // namespace store
} // namespace tell
