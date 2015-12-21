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

#include "LLVMScan.hpp"

#include "LLVMRowProjection.hpp"
#include "LLVMRowScan.hpp"

#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/alignment.hpp>
#include <crossbow/byte_buffer.hpp>
#include <crossbow/enum_underlying.hpp>
#include <crossbow/logger.hpp>

#include <llvm/ADT/ArrayRef.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Vectorize.h>

#include <array>
#include <sstream>
#include <string>

namespace tell {
namespace store {
namespace {

static const std::string gRowMaterializeFunctionName = "rowMaterialize.";

static const std::array<std::string, 3> gRowMaterializeParamNames = {{
    "recordData",
    "length",
    "destData"
}};

uint32_t memcpyWrapper(const char* src, uint32_t length, char* dest) {
    memcpy(dest, src, length);
    return length;
}

} // anonymous namespace

LLVMScanBase::LLVMScanBase()
        :  mCompilerModule("ScanQuery", mCompilerContext) {
    mCompilerModule.setDataLayout(mCompiler.getTargetMachine()->createDataLayout());
    mCompilerModule.setTargetTriple(mCompiler.getTargetMachine()->getTargetTriple().getTriple());
}

LLVMScanBase::~LLVMScanBase() {
    mCompiler.removeModule(mCompilerHandle);
}

void LLVMScanBase::finalizeScan() {
    using namespace llvm;

    for (auto& func : mCompilerModule) {
        // Add host CPU features
        func.addFnAttr(Attribute::NoUnwind);
        func.addFnAttr("target-cpu", mCompiler.getTargetMachine()->getTargetCPU());
        func.addFnAttr("target-features", mCompiler.getTargetMachine()->getTargetFeatureString());
    }

#ifndef NDEBUG
    LOG_INFO("Dumping LLVM scan code");
    mCompilerModule.dump();

    LOG_INFO("Verifying LLVM scan code");
    if (llvm::verifyModule(mCompilerModule, &llvm::dbgs())) {
        LOG_FATAL("Verifying LLVM scan code failed");
        std::terminate();
    }
#endif

    // Compile the module
    mCompilerHandle = mCompiler.addModule(&mCompilerModule);
}

LLVMRowScanBase::LLVMRowScanBase(const Record& record, std::vector<ScanQuery*> queries)
        : mQueries(std::move(queries)),
          mRowScanFun(nullptr) {
    buildScanAST(record);

    auto targetMachine = mCompiler.getTargetMachine();
    LLVMRowScanBuilder::createFunction(mCompilerModule, targetMachine, mScanAst);

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        auto q = mQueries[i];
        switch (q->queryType()) {
        case ScanQueryType::PROJECTION: {
            LLVMRowProjectionBuilder::createFunction(record, mCompilerModule, targetMachine, i, q);
        } break;

        case ScanQueryType::AGGREGATION: {
            prepareRowAggregationFunction(record, q, i);
        } break;
        default:
            break;
        }
    }
}

void LLVMRowScanBase::finalizeRowScan() {
    LOG_ASSERT(!mRowScanFun, "Scan already finalized");
    finalizeScan();
    mRowScanFun = mCompiler.findFunction<RowScanFun>(LLVMRowScanBuilder::FUNCTION_NAME);

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        if (mQueries[i]->queryType() == ScanQueryType::FULL) {
            mRowMaterializeFuns.emplace_back(&memcpyWrapper);
        } else {
            std::stringstream ss;
            ss << gRowMaterializeFunctionName << i;
            auto fun = mCompiler.findFunction<RowMaterializeFun>(ss.str());
            mRowMaterializeFuns.emplace_back(fun);
        }
    }
}

void LLVMRowScanBase::buildScanAST(const Record& record) {
    using namespace llvm;

    LLVMBuilder builder(mCompilerContext);

    mScanAst.numConjunct = mQueries.size();

    for (auto q : mQueries) {
        crossbow::buffer_reader queryReader(q->selection(), q->selectionLength());
        auto snapshot = q->snapshot();

        auto numColumns = queryReader.read<uint32_t>();

        QueryAST queryAst;
        queryAst.baseVersion = snapshot->baseVersion();
        queryAst.version = snapshot->version();
        queryAst.conjunctOffset = mScanAst.numConjunct;
        queryAst.numConjunct = queryReader.read<uint32_t>();
        queryAst.partitionModulo = queryReader.read<uint32_t>();
        queryAst.partitionNumber = queryReader.read<uint32_t>();

        if (queryAst.partitionModulo != 0) {
            mScanAst.needsKey = true;
        }

        for (decltype(numColumns) i = 0; i < numColumns; ++i) {
            auto currentColumn = queryReader.read<uint16_t>();
            auto numPredicates = queryReader.read<uint16_t>();
            queryReader.advance(4);

            // Add a new FieldAST if the field does not yet exist
            auto iter = mScanAst.fields.find(currentColumn);
            if (iter == mScanAst.fields.end()) {
                auto& fieldMeta = record.getFieldMeta(currentColumn);
                auto& field = fieldMeta.field;

                FieldAST fieldAst;
                fieldAst.id = currentColumn;
                fieldAst.isNotNull = field.isNotNull();
                fieldAst.isFixedSize = field.isFixedSized();
                fieldAst.type = field.type();
                fieldAst.offset = fieldMeta.offset;
                fieldAst.alignment = field.alignOf();
                fieldAst.size = field.staticSize();

                auto res = mScanAst.fields.emplace(currentColumn, std::move(fieldAst));
                LOG_ASSERT(res.second, "Field already in map");
                iter = res.first;
            }
            auto& fieldAst = iter->second;

            // Iterate over all predicates on the field
            for (decltype(numPredicates) j = 0; j < numPredicates; ++j) {
                auto predicateType = queryReader.read<PredicateType>();
                auto conjunct = queryAst.conjunctOffset + queryReader.read<uint8_t>();

                PredicateAST predicateAst(predicateType, conjunct);

                if (predicateType == PredicateType::IS_NULL || predicateType == PredicateType::IS_NOT_NULL) {
                    queryReader.advance(6);
                } else {
                    fieldAst.needsValue = true;

                    switch (fieldAst.type) {
                    case FieldType::SMALLINT: {
                        predicateAst.fixed.value = builder.getInt16(queryReader.read<int16_t>());
                        predicateAst.fixed.predicate = builder.getIntPredicate(predicateType);
                        predicateAst.fixed.isFloat = false;
                        queryReader.advance(4);
                    } break;

                    case FieldType::INT: {
                        queryReader.advance(2);
                        predicateAst.fixed.value = builder.getInt32(queryReader.read<int32_t>());
                        predicateAst.fixed.predicate = builder.getIntPredicate(predicateType);
                        predicateAst.fixed.isFloat = false;
                    } break;

                    case FieldType::BIGINT: {
                        queryReader.advance(6);
                        predicateAst.fixed.value = builder.getInt64(queryReader.read<int64_t>());
                        predicateAst.fixed.predicate = builder.getIntPredicate(predicateType);
                        predicateAst.fixed.isFloat = false;
                    } break;

                    case FieldType::FLOAT: {
                        queryReader.advance(2);
                        predicateAst.fixed.value = builder.getFloat(queryReader.read<float>());
                        predicateAst.fixed.predicate = builder.getFloatPredicate(predicateType);
                        predicateAst.fixed.isFloat = true;
                    } break;

                    case FieldType::DOUBLE: {
                        queryReader.advance(6);
                        predicateAst.fixed.value = builder.getDouble(queryReader.read<double>());
                        predicateAst.fixed.predicate = builder.getFloatPredicate(predicateType);
                        predicateAst.fixed.isFloat = true;
                    } break;

                    case FieldType::BLOB:
                    case FieldType::TEXT: {
                        queryReader.advance(2);
                        auto size = queryReader.read<uint32_t>();
                        auto data = queryReader.read(size);
                        queryReader.align(8u);

                        predicateAst.variable.size = size;
                        memset(predicateAst.variable.prefix, 0, sizeof(predicateAst.variable.prefix));
                        if (size > 0) {
                            memcpy(predicateAst.variable.prefix, data, size < sizeof(predicateAst.variable.prefix)
                                   ? size : sizeof(predicateAst.variable.prefix));

                            auto value = ConstantDataArray::get(builder.getContext(),
                                    makeArrayRef(reinterpret_cast<const uint8_t*>(data), size));
                            predicateAst.variable.value = new GlobalVariable(mCompilerModule, value->getType(), true,
                                    GlobalValue::PrivateLinkage, value);
                        }
                    } break;

                    default: {
                        LOG_ASSERT(false, "Invalid field");
                    } break;
                    }
                }
                fieldAst.predicates.emplace_back(std::move(predicateAst));
            }
        }

        mScanAst.numConjunct += queryAst.numConjunct;
        mScanAst.queries.emplace_back(std::move(queryAst));
    }
}

void LLVMRowScanBase::prepareRowAggregationFunction(const Record& srcRecord, ScanQuery* query, uint32_t index) {
    using namespace llvm;

    static constexpr size_t recordData = 0;
    static constexpr size_t destData = 2;

    LLVMBuilder builder(mCompilerContext);

    // Create function
    auto funcType = FunctionType::get(builder.getInt32Ty(), {
            builder.getInt8PtrTy(), // recordData
            builder.getInt32Ty(),   // length
            builder.getInt8PtrTy()  // destData
    }, false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gRowMaterializeFunctionName + Twine(index),
            &mCompilerModule);

    // Set arguments names
    std::array<Value*, 3> args;
    {
        decltype(gRowMaterializeParamNames.size()) idx = 0;
        for (auto iter = func->arg_begin(); idx != gRowMaterializeParamNames.size(); ++iter, ++idx) {
            iter->setName(gRowMaterializeParamNames[idx]);
            args[idx] = iter.operator ->();
        }
    }

    // Set noalias hints (data pointers are not allowed to overlap)
    func->setDoesNotAlias(1);
    func->setOnlyReadsMemory(1);
    func->setDoesNotAlias(3);

    // Build function
    auto bb = BasicBlock::Create(mCompilerContext, "entry", func);
    builder.SetInsertPoint(bb);

    auto& destRecord = query->record();
    Record::id_t j = 0u;
    auto end = query->aggregationEnd();
    for (auto i = query->aggregationBegin(); i != end; ++i, ++j) {

        uint16_t srcFieldIdx;
        AggregationType aggregationType;
        std::tie(srcFieldIdx, aggregationType) = *i;
        auto& srcFieldMeta = srcRecord.getFieldMeta(srcFieldIdx);
        auto& srcField = srcFieldMeta.field;
        auto srcFieldAlignment = srcField.alignOf();
        auto srcFieldPtrType = builder.getFieldPtrTy(srcField.type());
        auto srcFieldOffset = srcFieldMeta.offset;

        uint16_t destFieldIdx;
        destRecord.idOf(crossbow::to_string(j), destFieldIdx);
        auto& destFieldMeta = destRecord.getFieldMeta(destFieldIdx);
        auto& destField = destFieldMeta.field;
        auto destFieldAlignment = destField.alignOf();
        auto destFieldPtrType = builder.getFieldPtrTy(destField.type());
        auto destFieldOffset = destFieldMeta.offset;
        LOG_ASSERT(srcFieldOffset >= 0 && destFieldOffset >= 0, "Only fixed size supported at the moment");

        Value* srcNullBitmap = nullptr;
        if (!srcField.isNotNull()) {
            auto srcIdx = srcFieldIdx / 8u;
            auto srcBitIdx = srcFieldIdx % 8u;
            uint8_t srcMask = (0x1u << srcBitIdx);

            srcNullBitmap = (srcIdx == 0
                    ? args[recordData]
                    : builder.CreateInBoundsGEP(args[recordData], builder.getInt64(srcIdx)));
            srcNullBitmap = builder.CreateAnd(builder.CreateLoad(srcNullBitmap), builder.getInt8(srcMask));

            auto destIdx = destFieldIdx / 8u;
            auto destBitIdx = destFieldIdx % 8u;

            if (destBitIdx > srcBitIdx) {
                srcNullBitmap = builder.CreateShl(srcNullBitmap, builder.getInt64(destBitIdx - srcBitIdx));
            } else if (destBitIdx < srcBitIdx) {
                srcNullBitmap = builder.CreateLShr(srcNullBitmap, builder.getInt64(srcBitIdx - destBitIdx));
            }

            auto destNullBitmap = (destIdx == 0
                    ? args[destData]
                    : builder.CreateInBoundsGEP(args[destData], builder.getInt64(destIdx)));

            auto res = builder.CreateAnd(builder.CreateLoad(destNullBitmap), builder.CreateNeg(srcNullBitmap));
            builder.CreateStore(res, destNullBitmap);
        }

        Value* src = (srcFieldOffset == 0
                ? args[recordData]
                : builder.CreateInBoundsGEP(args[recordData], builder.getInt64(srcFieldOffset)));
        src = builder.CreateAlignedLoad(builder.CreateBitCast(src, srcFieldPtrType), srcFieldAlignment);

        auto destPtr = (destFieldOffset == 0
                ? args[destData]
                : builder.CreateInBoundsGEP(args[destData], builder.getInt64(destFieldOffset)));
        destPtr = builder.CreateBitCast(destPtr, destFieldPtrType);
        Value* dest = builder.CreateAlignedLoad(destPtr, destFieldAlignment);

        if (!srcField.isNotNull()) {
            srcNullBitmap = builder.CreateICmp(CmpInst::ICMP_EQ, srcNullBitmap, builder.getInt8(0));
        }

        auto isFloat = (srcField.type() == FieldType::FLOAT) || (srcField.type() == FieldType::DOUBLE);

        switch (aggregationType) {
        case AggregationType::MIN: {
            auto cond = (isFloat
                    ? builder.CreateFCmp(CmpInst::FCMP_OLT, src, dest)
                    : builder.CreateICmp(CmpInst::ICMP_SLT, src, dest));
            if (!srcField.isNotNull()) {
                cond = builder.CreateAnd(cond, srcNullBitmap);
            }
            dest = builder.CreateSelect(cond, src, dest);
        } break;

        case AggregationType::MAX: {
            auto cond = (isFloat
                    ? builder.CreateFCmp(CmpInst::FCMP_OGT, src, dest)
                    : builder.CreateICmp(CmpInst::ICMP_SGT, src, dest));
            if (!srcField.isNotNull()) {
                cond = builder.CreateAnd(cond, srcNullBitmap);
            }
            dest = builder.CreateSelect(cond, src, dest);
        } break;

        case AggregationType::SUM: {
            if (srcField.type() == FieldType::SMALLINT || srcField.type() == FieldType::INT) {
                src = builder.CreateSExt(src, builder.getInt64Ty());
            } else if (srcField.type() == FieldType::FLOAT) {
                src = builder.CreateFPExt(src, builder.getDoubleTy());
            }

            auto res = (isFloat
                    ? builder.CreateFAdd(dest, src)
                    : builder.CreateAdd(dest, src));
            if (!srcField.isNotNull()) {
                dest = builder.CreateSelect(srcNullBitmap, res, dest);
            } else {
                dest = res;
            }
        } break;

        case AggregationType::CNT: {
            if (!srcField.isNotNull()) {
                dest = builder.CreateAdd(dest, builder.CreateZExt(srcNullBitmap, builder.getInt64Ty()));
            } else {
                dest = builder.CreateAdd(dest, builder.getInt64(1));
            }
        } break;

        default:
            break;
        }

        builder.CreateAlignedStore(dest, destPtr, destFieldAlignment);
    }

    builder.CreateRet(builder.getInt32(destRecord.staticSize()));
}

LLVMRowScanProcessorBase::LLVMRowScanProcessorBase(const Record& record, const std::vector<ScanQuery*>& queries,
        LLVMRowScanBase::RowScanFun rowScanFunc,
        const std::vector<LLVMRowScanBase::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts)
        : mRecord(record),
          mRowScanFun(rowScanFunc),
          mRowMaterializeFuns(rowMaterializeFuns),
          mNumConjuncts(numConjuncts) {
    LOG_ASSERT(mNumConjuncts >= queries.size(), "More queries than conjuncts");

    mQueries.reserve(queries.size());
    for (auto q : queries) {
        mQueries.emplace_back(q->createProcessor());
    }
}

void LLVMRowScanProcessorBase::processRowRecord(uint64_t key, uint64_t validFrom, uint64_t validTo, const char* data,
        uint32_t length) {
    if (mResult.size() < mNumConjuncts) {
        mResult.resize(mNumConjuncts, 0u);
    }
    mRowScanFun(key, validFrom, validTo, data, &mResult.front());

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        // Check if the selection string matches the record
        if (mResult[i] == 0) {
            continue;
        }

        mQueries[i].writeRecord(key, length, validFrom, validTo, [this, i, data, length] (char* dest) {
            return mRowMaterializeFuns[i](data, length, dest);
        });
    }
}

} // namespace store
} // namespace tell
