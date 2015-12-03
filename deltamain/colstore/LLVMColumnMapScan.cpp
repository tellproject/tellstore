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

namespace tell {
namespace store {
namespace deltamain {

const std::string LLVMColumnMapScanBuilder::FUNCTION_NAME = "columnScan";

LLVMColumnMapScanBuilder::LLVMColumnMapScanBuilder(llvm::Module& module, llvm::TargetMachine* target)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()),
                FUNCTION_NAME),
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
    mFunction->setDoesNotAlias(5);
    mFunction->setOnlyReadsMemory(5);
    mFunction->setDoesNotAlias(9);
}

void LLVMColumnMapScanBuilder::buildScan(const ScanAST& scanAst) {
    if (scanAst.queries.empty()) {
        CreateRetVoid();
        return;
    }

    // Field evaluation
    mScalarConjunctsGenerated.resize(scanAst.numConjunct, false);
    mVectorConjunctsGenerated.resize(scanAst.numConjunct, false);
    for (auto& fieldAst : scanAst.fields) {
        buildField(fieldAst.second);
    }
    mScalarConjunctsGenerated.clear();
    mVectorConjunctsGenerated.clear();

    // Query evaluation
    buildQuery(scanAst.needsKey, scanAst.queries);

    // Result evaluation
    buildResult(scanAst.queries);

    CreateRetVoid();
}

void LLVMColumnMapScanBuilder::buildField(const FieldAST& fieldAst) {
    // Load the start pointer of the column
    llvm::Value* recordPtr = nullptr;
    if (fieldAst.needsValue) {
        recordPtr = getParam(recordData);
        if (fieldAst.offset > 0) {
            recordPtr = CreateInBoundsGEP(recordPtr, createConstMul(getParam(count), fieldAst.offset));
        }
        recordPtr = CreateBitCast(recordPtr, getFieldPtrTy(fieldAst.type));
    }

    // Vectorized field evaluation
    auto vectorSize = mRegisterWidth / (fieldAst.size * 8);
    auto vectorEndIdx = buildFixedFieldEvaluation(recordPtr, getParam(startIdx), vectorSize, mVectorConjunctsGenerated,
            fieldAst, llvm::Twine("vector"));

    // Scalar field evaluation
    buildFixedFieldEvaluation(recordPtr, vectorEndIdx, 1, mScalarConjunctsGenerated, fieldAst, llvm::Twine("scalar"));
}

llvm::Value* LLVMColumnMapScanBuilder::buildFixedFieldEvaluation(llvm::Value* recordPtr, llvm::Value* startIdx,
        uint64_t vectorSize, std::vector<uint8_t>& conjunctsGenerated, const FieldAST& fieldAst,
        const llvm::Twine& name) {
    return createLoop(startIdx, getParam(endIdx), vectorSize, "col." + llvm::Twine(fieldAst.id) + "." + name,
            [this, recordPtr, vectorSize, &conjunctsGenerated, &fieldAst] (llvm::Value* idx) {
        auto fieldTy = getFieldVectorTy(fieldAst.type, vectorSize);
        auto conjunctTy = getInt8VectorTy(vectorSize);

        // Load the field value from the record
        llvm::Value* lhs = recordPtr;
        if (lhs) {
            lhs = CreateInBoundsGEP(lhs, idx);
            lhs = CreateBitCast(lhs, fieldTy->getPointerTo());
            lhs = CreateAlignedLoad(lhs, fieldAst.alignment);
        }

        // Evaluate all predicates attached to this field
        for (auto& predicateAst : fieldAst.predicates) {
            LOG_ASSERT(lhs != nullptr, "lhs must not be null for this kind of comparison");
            auto& rhsAst = predicateAst.fixed;

            // Execute the comparison
            auto rhs = getVector(vectorSize, rhsAst.value);
            auto res = (rhsAst.isFloat
                ? CreateFCmp(rhsAst.predicate, lhs, rhs)
                : CreateICmp(rhsAst.predicate, lhs, rhs));
            res = CreateZExtOrBitCast(res, conjunctTy);

            // Store resulting conjunct value
            auto conjunctPtr = CreateAdd(createConstMul(getParam(count), predicateAst.conjunct), idx);
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

            // Evaluate key % partitionModulo == partitionNumber
            if (query.partitionModulo != 0u) {
                LOG_ASSERT(needsKey, "No partitioning in AST");
                auto keyRes = CreateICmp(llvm::CmpInst::ICMP_EQ,
                        createConstMod(key, query.partitionModulo, vectorSize),
                        getInt64Vector(vectorSize, query.partitionNumber));
                res = CreateAnd(res, keyRes);
            }
            res = CreateZExtOrBitCast(res, conjunctTy);

            // Store temporary result value
            auto resultPtr = idx;
            if (i > 0) {
                resultPtr = CreateAdd(createConstMul(getParam(count), i), resultPtr);
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
            lhsPtr = CreateAdd(createConstMul(getParam(count), dest), lhsPtr);
        }
        lhsPtr = CreateInBoundsGEP(getParam(resultData), lhsPtr);
        lhsPtr = CreateBitCast(lhsPtr, conjunctTy->getPointerTo());
        auto lhs = CreateAlignedLoad(lhsPtr, 1u);

        // Load source conjunct
        auto rhsPtr = idx;
        if (src > 0) {
            rhsPtr = CreateAdd(createConstMul(getParam(count), src), rhsPtr);
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
