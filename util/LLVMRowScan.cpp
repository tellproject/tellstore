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

#include "LLVMRowScan.hpp"

#include "LLVMScan.hpp"

#include <util/ScanQuery.hpp>

#include <llvm/IR/Module.h>

namespace tell {
namespace store {

const std::string LLVMRowScanBuilder::FUNCTION_NAME = "rowScan";

LLVMRowScanBuilder::LLVMRowScanBuilder(llvm::Module& module, llvm::TargetMachine* target)
        : FunctionBuilder(module, target, buildReturnTy(module.getContext()), buildParamTy(module.getContext()),
                FUNCTION_NAME) {
    // Set noalias hints (data pointers are not allowed to overlap)
    mFunction->setDoesNotAlias(4);
    mFunction->setOnlyReadsMemory(4);
    mFunction->setDoesNotAlias(5);
}

void LLVMRowScanBuilder::buildScan(const ScanAST& scanAst) {
    if (scanAst.queries.empty()) {
        CreateRetVoid();
        return;
    }

    mConjunctsGenerated.resize(scanAst.numConjunct, false);
    for (auto& f : scanAst.fields) {
        auto& fieldAst = f.second;

        // Load the null status of the value if it can be null
        llvm::Value* nullValue = nullptr;
        if (!fieldAst.isNotNull) {
            auto nullData = getParam(recordData);
            if (fieldAst.nullIdx != 0) {
                nullData = CreateInBoundsGEP(nullData, getInt64(fieldAst.nullIdx));
            }
            nullValue = CreateAlignedLoad(nullData, 1u);
        }

        // Load the field value from the record
        llvm::Value* lhs = nullptr;
        llvm::Value* length = nullptr;
        if (fieldAst.needsValue) {
            lhs = getParam(recordData);
            if (fieldAst.offset != 0) {
                lhs = CreateInBoundsGEP(lhs, getInt64(fieldAst.offset));
            }
            lhs = CreateBitCast(lhs, getFieldPtrTy(fieldAst.type));
            lhs = CreateAlignedLoad(lhs, fieldAst.alignment);
            if (!fieldAst.isFixedSize) {
                auto endOffset = CreateInBoundsGEP(getParam(recordData), getInt64(fieldAst.offset + sizeof(uint32_t)));
                endOffset = CreateBitCast(endOffset, getFieldPtrTy(fieldAst.type));
                endOffset = CreateAlignedLoad(endOffset, fieldAst.alignment);

                length = CreateSub(endOffset, lhs);

                lhs = CreateInBoundsGEP(getParam(recordData), lhs);
            }
        }

        // Evaluate all predicates attached to this field
        for (decltype(fieldAst.predicates.size()) i = 0; i < fieldAst.predicates.size(); ++i) {
            auto& predicateAst = fieldAst.predicates[i];

            llvm::Value* res;
            if (predicateAst.type == PredicateType::IS_NULL) {
                // Check if the field is null
                res = nullValue;
            } else if (predicateAst.type == PredicateType::IS_NOT_NULL) {
                res = CreateXor(nullValue, getInt8(1));
            } else if (fieldAst.isFixedSize) {
                LOG_ASSERT(lhs != nullptr, "lhs must not be null for this kind of comparison");
                auto& rhsAst = predicateAst.fixed;

                // Execute the comparison
                res = (rhsAst.isFloat
                        ? CreateFCmp(rhsAst.predicate, lhs, rhsAst.value)
                        : CreateICmp(rhsAst.predicate, lhs, rhsAst.value));
                res = CreateZExtOrBitCast(res, getInt8Ty());

                // The predicate evaluates to false if the value is null
                if (nullValue) {
                    res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
                }
            } else {
                LOG_ASSERT(lhs != nullptr, "lhs must not be null for this kind of comparison");
                LOG_ASSERT(length != nullptr, "length must not be null for this kind of comparison");
                auto& rhsAst = predicateAst.variable;

                // Execute the comparison
                switch (predicateAst.type) {
                case PredicateType::EQUAL:
                case PredicateType::NOT_EQUAL: {
                    auto negateResult = (predicateAst.type == PredicateType::NOT_EQUAL);
                    if (rhsAst.size == 0) {
                        res = CreateICmp((negateResult ? llvm::CmpInst::ICMP_NE : llvm::CmpInst::ICMP_EQ), length,
                                getInt32(0));
                    } else {
                        res = CreateICmp(llvm::CmpInst::ICMP_EQ, length, getInt32(rhsAst.size));

                        auto rhsStart = CreateInBoundsGEP(rhsAst.value->getValueType(), rhsAst.value,
                                { getInt64(0), getInt32(0) });
                        auto rhsEnd = CreateGEP(rhsAst.value->getValueType(), rhsAst.value,
                                { getInt64(1), getInt32(0) });

                        res = createMemCmp(res, lhs, rhsStart, rhsEnd,
                                "col." + llvm::Twine(fieldAst.id) + "." + llvm::Twine(i));
                        if (negateResult) {
                            res = CreateNot(res);
                        }
                    }
                } break;

                case PredicateType::PREFIX_LIKE:
                case PredicateType::PREFIX_NOT_LIKE: {
                    auto negateResult = (predicateAst.type == PredicateType::PREFIX_NOT_LIKE);
                    if (rhsAst.size == 0) {
                        res = (negateResult ? getFalse() : getTrue());
                    } else {
                        res = CreateICmp(llvm::CmpInst::ICMP_UGE, length, getInt32(rhsAst.size));
                        auto rhsStart = CreateInBoundsGEP(rhsAst.value->getValueType(), rhsAst.value,
                                { getInt64(0), getInt32(0) });
                        auto rhsEnd = CreateGEP(rhsAst.value->getValueType(), rhsAst.value,
                                { getInt64(1), getInt32(0) });

                        res = createMemCmp(res, lhs, rhsStart, rhsEnd,
                                "col." + llvm::Twine(fieldAst.id) + "." + llvm::Twine(i));
                        if (negateResult) {
                            res = CreateNot(res);
                        }
                    }
                } break;

                case PredicateType::POSTFIX_LIKE:
                case PredicateType::POSTFIX_NOT_LIKE: {
                    auto negateResult = (predicateAst.type == PredicateType::POSTFIX_NOT_LIKE);
                    if (rhsAst.size == 0) {
                        res = (negateResult ? getFalse() : getTrue());
                    } else {
                        res = createPostfixMemCmp(lhs, length, rhsAst.value, rhsAst.size,
                                "col." + llvm::Twine(fieldAst.id) + "." + llvm::Twine(i));
                        if (negateResult) {
                            res = CreateNot(res);
                        }
                    }
                } break;

                default: {
                    LOG_ASSERT(false, "Unknown predicate");
                    res = nullptr;
                } break;
                }

                res = CreateZExtOrBitCast(res, getInt8Ty());

                // The predicate evaluates to false if the value is null
                if (nullValue) {
                    res = CreateAnd(res, CreateXor(nullValue, getInt8(1)));
                }
            }

            // Store resulting conjunct value
            auto conjunctPtr = CreateInBoundsGEP(getParam(resultData), getInt64(predicateAst.conjunct));
            if (mConjunctsGenerated[predicateAst.conjunct]) {
                res = CreateOr(CreateAlignedLoad(conjunctPtr, 1u), res);
            } else {
                mConjunctsGenerated[predicateAst.conjunct] = true;
            }
            CreateAlignedStore(res, conjunctPtr, 1u);
        }
    }
    mConjunctsGenerated.clear();

    for (decltype(scanAst.queries.size()) i = 0; i < scanAst.queries.size(); ++i) {
        auto& query = scanAst.queries[i];

        // Evaluate validFrom <= version && validTo > baseVersion
        auto validFromRes = CreateICmp(llvm::CmpInst::ICMP_ULE, getParam(validFrom), getInt64(query.version));
        auto validToRes = CreateICmp(llvm::CmpInst::ICMP_UGT, getParam(validTo), getInt64(query.baseVersion));
        auto res = CreateAnd(validFromRes, validToRes);

        // Evaluate key % partitionModulo == partitionNumber
        if (query.partitionModulo != 0u) {
            auto keyRes = CreateICmp(llvm::CmpInst::ICMP_EQ,
                    createConstMod(getParam(key), query.partitionModulo),
                    getInt64(query.partitionNumber));
            res = CreateAnd(res, keyRes);
        }
        res = CreateZExtOrBitCast(res, getInt8Ty());

        // Merge conjuncts
        for (decltype(query.numConjunct) j = 0; j < query.numConjunct; ++j) {
            auto conjunctPtr = CreateInBoundsGEP(getParam(resultData), getInt64(query.conjunctOffset + j));
            res = CreateAnd(res, CreateAlignedLoad(conjunctPtr, 1u));
        }

        // Store final result conjunct
        auto resultPtr = CreateInBoundsGEP(getParam(resultData), getInt64(i));
        CreateAlignedStore(res, resultPtr, 1u);
    }

    CreateRetVoid();
}

} // namespace store
} // namespace tell
