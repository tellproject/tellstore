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

#include "ColumnMapScanProcessor.hpp"

#include "ColumnMapContext.hpp"
#include "ColumnMapPage.hpp"
#include "ColumnMapRecord.hpp"

#include <deltamain/Table.hpp>

#include <util/LLVMBuilder.hpp>

#include <llvm/ADT/ArrayRef.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
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
namespace deltamain {
namespace {

static const std::string gColumnScanFunctionName = "columnScan";

static const std::array<std::string, 9> gColumnScanParamNames = {{
    "keyData",
    "validFromData",
    "validToData",
    "recordData",
    "heapData",
    "count",
    "startIdx",
    "endIdx",
    "resultData"
}};

static const std::string gColumnMaterializeFunctionName = "columnMaterialize.";

static const std::array<std::string, 5> gColumnProjectionParamNames = {{
    "recordData",
    "heapData",
    "count",
    "idx",
    "destData"
}};

static const std::array<std::string, 7> gColumnAggregationParamNames = {{
    "recordData",
    "heapData",
    "count",
    "startIdx",
    "endIdx",
    "resultData",
    "destData"
}};

struct QueryState {
    QueryState(const ScanQuery* query, uint32_t _conjunctOffset)
            : queryReader(query->selection(), query->selectionLength()),
              snapshot(query->snapshot()),
              conjunctOffset(_conjunctOffset),
              numConjunct(0u),
              partitionModulo(0u),
              partitionNumber(0u),
              currentColumn(0u) {
    }

    crossbow::buffer_reader queryReader;

    const commitmanager::SnapshotDescriptor* snapshot;

    uint32_t conjunctOffset;

    uint32_t numConjunct;

    uint32_t partitionModulo;

    uint32_t partitionNumber;

    uint16_t currentColumn;
};

} // anonymous namespace

ColumnMapScan::ColumnMapScan(Table<ColumnMapContext>* table, std::vector<ScanQuery*> queries)
        : LLVMRowScanBase(table->record(), std::move(queries)),
          mTable(table),
          mColumnScanFun(nullptr) {
    prepareColumnScanFunction(table->record());

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        auto q = mQueries[i];
        switch (q->queryType()) {
        case ScanQueryType::PROJECTION: {
            prepareColumnProjectionFunction(table->record(), q, i);
        } break;

        case ScanQueryType::AGGREGATION: {
            prepareColumnAggregationFunction(table->record(), q, i);
        } break;
        default:
            break;
        }
    }

    finalizeRowScan();

    mColumnScanFun = mCompiler.findFunction<ColumnScanFun>(gColumnScanFunctionName);

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        switch (mQueries[i]->queryType()) {
        case ScanQueryType::FULL: {
            mColumnProjectionFuns.emplace_back(nullptr);
            mColumnAggregationFuns.emplace_back(nullptr);
        } break;

        case ScanQueryType::PROJECTION: {
            std::stringstream ss;
            ss << gColumnMaterializeFunctionName << i;
            auto fun = mCompiler.findFunction<ColumnProjectionFun>(ss.str());
            mColumnProjectionFuns.emplace_back(fun);
            mColumnAggregationFuns.emplace_back(nullptr);
        } break;

        case ScanQueryType::AGGREGATION: {
            std::stringstream ss;
            ss << gColumnMaterializeFunctionName << i;
            auto fun = mCompiler.findFunction<ColumnAggregationFun>(ss.str());
            mColumnAggregationFuns.emplace_back(fun);
            mColumnProjectionFuns.emplace_back(nullptr);
        } break;
        }
    }
}

std::vector<std::unique_ptr<ColumnMapScanProcessor>> ColumnMapScan::startScan(size_t numThreads) {
    return mTable->startScan(numThreads, mQueries, mColumnScanFun, mColumnProjectionFuns, mColumnAggregationFuns,
            mRowScanFun, mRowMaterializeFuns, mNumConjuncts);
}

void ColumnMapScan::prepareColumnScanFunction(const Record& record) {
    using namespace llvm;

    static constexpr size_t keyData = 0;
    static constexpr size_t validFromData = 1;
    static constexpr size_t validToData = 2;
    static constexpr size_t recordData = 3;
    static constexpr size_t heapData = 4;
    static constexpr size_t count = 5;
    static constexpr size_t startIdx = 6;
    static constexpr size_t endIdx = 7;
    static constexpr size_t resultData = 8;

    LLVMBuilder builder(mCompilerContext);

    // Create function
    auto funcType = FunctionType::get(builder.getVoidTy(), {
            builder.getInt64PtrTy(),    // keyData
            builder.getInt64PtrTy(),    // validFromData
            builder.getInt64PtrTy(),    // validToData
            builder.getInt8PtrTy(),     // recordData
            builder.getInt8PtrTy(),     // heapData
            builder.getInt64Ty(),       // count
            builder.getInt64Ty(),       // startIdx
            builder.getInt64Ty(),       // endIdx
            builder.getInt8PtrTy()      // resultData
    }, false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gColumnScanFunctionName, &mCompilerModule);

    auto targetInfo = mCompiler.getTargetMachine()->getTargetIRAnalysis().run(*func);

    // Set arguments names
    std::array<Value*, 9> args;
    {
        decltype(gColumnScanParamNames.size()) idx = 0;
        for (auto iter = func->arg_begin(); idx != gColumnScanParamNames.size(); ++iter, ++idx) {
            iter->setName(gColumnScanParamNames[idx]);
            args[idx] = iter.operator ->();
        }
    }

    // Set noalias hints (data pointers are not allowed to overlap)
    func->setDoesNotAlias(1);
    func->setOnlyReadsMemory(1);
    func->setDoesNotAlias(2);
    func->setOnlyReadsMemory(2);
    func->setDoesNotAlias(3);
    func->setOnlyReadsMemory(3);
    func->setDoesNotAlias(4);
    func->setOnlyReadsMemory(4);
    func->setDoesNotAlias(5);
    func->setOnlyReadsMemory(5);
    func->setDoesNotAlias(9);

    // Build function
    auto entryBlock = BasicBlock::Create(mCompilerContext, "entry", func);
    builder.SetInsertPoint(entryBlock);

    if (mQueries.size() == 0) {
        builder.CreateRetVoid();
        return;
    }
    std::vector<QueryState> queryBuffers;
    queryBuffers.reserve(mQueries.size());

    uint32_t numConjuncts = mQueries.size();
    for (auto q : mQueries) {
        queryBuffers.emplace_back(q, numConjuncts);

        auto& state = queryBuffers.back();
        auto& queryReader = state.queryReader;
        auto numColumns = queryReader.read<uint32_t>();

        state.numConjunct = queryReader.read<uint32_t>();
        state.partitionModulo = queryReader.read<uint32_t>();
        state.partitionNumber = queryReader.read<uint32_t>();
        if (numColumns != 0) {
            state.currentColumn = queryReader.read<uint16_t>();
        } else {
            state.currentColumn = std::numeric_limits<uint16_t>::max();
        }

        numConjuncts += state.numConjunct;
    }
    LOG_ASSERT(mNumConjuncts == numConjuncts, "Number of conjuncts differ from row scan function");

    std::vector<uint8_t> conjunctsGenerated(mNumConjuncts, false);

    auto currentColumn = std::numeric_limits<uint16_t>::max();
    decltype(queryBuffers.size()) nextQueryIdx = 0;
    while (true) {
        // Find the next query with the nearest column
        for (decltype(queryBuffers.size()) i = 0; i < queryBuffers.size(); ++i) {
            auto& state = queryBuffers.at(i);
            if (state.currentColumn < currentColumn) {
                currentColumn = state.currentColumn;
                nextQueryIdx = i;
            }
        }
        if (currentColumn == std::numeric_limits<uint16_t>::max()) {
            break;
        }

        // -> auto src = page->recordData() + page->count * mFieldOffsets[idx];
        auto& field = record.getFieldMeta(currentColumn).first;
        auto fieldOffset = record.getFieldMeta(currentColumn).second;
        auto fieldAlignment = field.alignOf();
        auto src = (fieldOffset == 0
                ? args[recordData]
                : builder.CreateInBoundsGEP(args[recordData], builder.createConstMul(args[count], fieldOffset)));
        src = builder.CreateBitCast(src, builder.getFieldPtrTy(field.type()));

        auto vectorSize = static_cast<uint64_t>(targetInfo.getRegisterBitWidth(true)) / (field.staticSize() * 8);

        auto vectorCount = builder.CreateSub(args[endIdx], args[startIdx]);
        vectorCount = builder.CreateAnd(vectorCount, builder.getInt64(-vectorSize));
        auto vectorEndIdx = builder.CreateAdd(args[startIdx], vectorCount);

        auto previousBlock = builder.GetInsertBlock();
        auto vectorBodyBlock = BasicBlock::Create(mCompilerContext, "col.vectorbody." + Twine(currentColumn), func);
        auto vectorEndBlock = BasicBlock::Create(mCompilerContext, "col.vectorend." + Twine(currentColumn), func);
        auto scalarBodyBlock = BasicBlock::Create(mCompilerContext, "col.scalarbody." + Twine(currentColumn), func);
        auto scalarEndBlock = BasicBlock::Create(mCompilerContext, "col.scalarend." + Twine(currentColumn), func);
        builder.CreateCondBr(builder.CreateICmp(CmpInst::ICMP_NE, vectorCount, builder.getInt64(0)),
                vectorBodyBlock, vectorEndBlock);

        // Scalar -> auto i = startIdx;
        builder.SetInsertPoint(scalarBodyBlock);
        auto scalarLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
        scalarLoopCounter->addIncoming(vectorEndIdx, vectorEndBlock);

        auto scalarSrc = builder.CreateInBoundsGEP(src, scalarLoopCounter);
        auto scalarFieldValue = builder.CreateAlignedLoad(scalarSrc, fieldAlignment);

        // Vector -> auto i = startIdx;
        builder.SetInsertPoint(vectorBodyBlock);
        auto vectorLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
        vectorLoopCounter->addIncoming(args[startIdx], previousBlock);

        auto vectorSrc = builder.CreateInBoundsGEP(src, vectorLoopCounter);
        vectorSrc = builder.CreateBitCast(vectorSrc, builder.getFieldVectorPtrTy(vectorSize, field.type()));
        auto vectorFieldValue = builder.CreateAlignedLoad(vectorSrc, fieldAlignment);

        for (decltype(queryBuffers.size()) i = nextQueryIdx; i < queryBuffers.size(); ++i) {
            auto& state = queryBuffers.at(i);
            if (state.currentColumn != currentColumn) {
                continue;
            }
            auto& queryReader = state.queryReader;

            auto numPredicates = queryReader.read<uint16_t>();
            queryReader.advance(4);

            for (decltype(numPredicates) j = 0; j < numPredicates; ++j) {
                auto predicateType = queryReader.read<PredicateType>();
                auto conjunctPosition = state.conjunctOffset + queryReader.read<uint8_t>();

                bool isFloat;
                Constant* compareValue;
                switch (field.type()) {
                case FieldType::SMALLINT: {
                    isFloat = false;
                    compareValue = builder.getInt16(queryReader.read<int16_t>());
                    queryReader.advance(4);
                } break;

                case FieldType::INT: {
                    isFloat = false;
                    queryReader.advance(2);
                    compareValue = builder.getInt32(queryReader.read<int32_t>());
                } break;

                case FieldType::BIGINT: {
                    isFloat = false;
                    queryReader.advance(6);
                    compareValue = builder.getInt64(queryReader.read<int64_t>());
                } break;

                case FieldType::FLOAT: {
                    isFloat = true;
                    queryReader.advance(2);
                    compareValue = builder.getFloat(queryReader.read<float>());
                } break;

                case FieldType::DOUBLE: {
                    isFloat = true;
                    queryReader.advance(6);
                    compareValue = builder.getDouble(queryReader.read<double>());
                } break;

                default: {
                    LOG_ASSERT(false, "Only fixed size fields are supported (right now)");
                    isFloat = false;
                    compareValue = nullptr;
                } break;
                }

                CmpInst::Predicate predicate;
                switch (predicateType) {
                case PredicateType::EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_OEQ : CmpInst::ICMP_EQ);
                } break;

                case PredicateType::NOT_EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_ONE : CmpInst::ICMP_NE);
                } break;

                case PredicateType::LESS: {
                    predicate = (isFloat ? CmpInst::FCMP_OLT : CmpInst::ICMP_SLT);
                } break;

                case PredicateType::LESS_EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_OLE : CmpInst::ICMP_SLE);
                } break;

                case PredicateType::GREATER: {
                    predicate = (isFloat ? CmpInst::FCMP_OGT : CmpInst::ICMP_SGT);
                } break;

                case PredicateType::GREATER_EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_OGE : CmpInst::ICMP_SGE);
                } break;

                default: {
                    LOG_ASSERT(false, "Predicate not supported (right now)");
                    predicate = (isFloat ? CmpInst::BAD_FCMP_PREDICATE : CmpInst::BAD_ICMP_PREDICATE);
                } break;
                }

                // Scalar -> Compare
                builder.SetInsertPoint(scalarBodyBlock);
                auto scalarComp = (isFloat
                        ? builder.CreateFCmp(predicate, scalarFieldValue, compareValue)
                        : builder.CreateICmp(predicate, scalarFieldValue, compareValue));
                scalarComp = builder.CreateZExtOrBitCast(scalarComp, builder.getInt8Ty());

                auto scalarConjunctIndex = builder.CreateAdd(
                        builder.createConstMul(args[count], conjunctPosition),
                        scalarLoopCounter);
                auto scalarConjunctElement = builder.CreateInBoundsGEP(args[resultData], scalarConjunctIndex);

                // Vector -> Compare
                builder.SetInsertPoint(vectorBodyBlock);
                auto vectorCompareValue = ConstantVector::getSplat(vectorSize, compareValue);
                auto vectorComp = (isFloat
                        ? builder.CreateFCmp(predicate, vectorFieldValue, vectorCompareValue)
                        : builder.CreateICmp(predicate, vectorFieldValue, vectorCompareValue));
                vectorComp = builder.CreateZExtOrBitCast(vectorComp, VectorType::get(builder.getInt8Ty(), vectorSize));

                auto vectorConjunctIndex = builder.CreateAdd(
                        builder.createConstMul(args[count], conjunctPosition),
                        vectorLoopCounter);
                auto vectorConjunctElement = builder.CreateInBoundsGEP(args[resultData], vectorConjunctIndex);
                vectorConjunctElement = builder.CreateBitCast(vectorConjunctElement,
                        builder.getInt8VectorPtrTy(vectorSize));

                // -> res[i] = res[i] | comp;
                if (conjunctsGenerated[conjunctPosition]) {
                    builder.SetInsertPoint(scalarBodyBlock);
                    scalarComp = builder.CreateOr(builder.CreateAlignedLoad(scalarConjunctElement, 1u), scalarComp);

                    builder.SetInsertPoint(vectorBodyBlock);
                    vectorComp = builder.CreateOr(builder.CreateAlignedLoad(vectorConjunctElement, 1u), vectorComp);
                } else {
                    conjunctsGenerated[conjunctPosition] = true;
                }
                builder.SetInsertPoint(scalarBodyBlock);
                builder.CreateAlignedStore(scalarComp, scalarConjunctElement, 1u);

                builder.SetInsertPoint(vectorBodyBlock);
                builder.CreateAlignedStore(vectorComp, vectorConjunctElement, 1u);
            }

            if (queryReader.exhausted()) {
                state.currentColumn = std::numeric_limits<uint16_t>::max();
            } else {
                state.currentColumn = queryReader.read<uint16_t>();
            }
        }

        // -> i += 1;
        builder.SetInsertPoint(scalarBodyBlock);
        auto scalarNextVar = builder.CreateAdd(scalarLoopCounter, builder.getInt64(1));
        scalarLoopCounter->addIncoming(scalarNextVar, scalarBodyBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        auto vectorNextVar = builder.CreateAdd(vectorLoopCounter, builder.getInt64(vectorSize));
        vectorLoopCounter->addIncoming(vectorNextVar, vectorBodyBlock);

        // i != endIdx
        builder.SetInsertPoint(scalarBodyBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, scalarNextVar, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorNextVar, vectorEndIdx),
                vectorBodyBlock, vectorEndBlock);

        builder.SetInsertPoint(vectorEndBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorEndIdx, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        builder.SetInsertPoint(scalarEndBlock);

        currentColumn = std::numeric_limits<uint16_t>::max();
    }

    {
        auto previousBlock = builder.GetInsertBlock();

        auto vectorSize = static_cast<uint64_t>(targetInfo.getRegisterBitWidth(true)) / (sizeof(uint64_t) * 8);

        auto vectorCount = builder.CreateSub(args[endIdx], args[startIdx]);
        vectorCount = builder.CreateAnd(vectorCount, builder.getInt64(-vectorSize));
        auto vectorEndIdx = builder.CreateAdd(args[startIdx], vectorCount);

        auto vectorBodyBlock = BasicBlock::Create(mCompilerContext, "check.vectorbody", func);
        auto vectorEndBlock = BasicBlock::Create(mCompilerContext, "check.vectorend", func);
        auto scalarBodyBlock = BasicBlock::Create(mCompilerContext, "check.scalarbody", func);
        auto scalarEndBlock = BasicBlock::Create(mCompilerContext, "check.scalarend", func);
        builder.CreateCondBr(builder.CreateICmp(CmpInst::ICMP_NE, vectorCount, builder.getInt64(0)),
                vectorBodyBlock, vectorEndBlock);

        // -> auto i = startIdx;
        builder.SetInsertPoint(scalarBodyBlock);
        auto scalarLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
        scalarLoopCounter->addIncoming(vectorEndIdx, vectorEndBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        auto vectorLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
        vectorLoopCounter->addIncoming(args[startIdx], previousBlock);

        for (decltype(queryBuffers.size()) i = 0; i < queryBuffers.size(); ++i) {
            auto& state = queryBuffers.at(i);
            auto snapshot = state.snapshot;

            // Scalar -> validFrom <= version && validTo > baseVersion
            builder.SetInsertPoint(scalarBodyBlock);
            auto scalarValidFrom = builder.CreateInBoundsGEP(args[validFromData], scalarLoopCounter);
            scalarValidFrom = builder.CreateAlignedLoad(scalarValidFrom, 8u);
            scalarValidFrom = builder.CreateICmp(CmpInst::ICMP_ULE, scalarValidFrom,
                    builder.getInt64(snapshot->version()));

            auto scalarValidTo = builder.CreateInBoundsGEP(args[validToData], scalarLoopCounter);
            scalarValidTo = builder.CreateAlignedLoad(scalarValidTo, 8u);
            scalarValidTo = builder.CreateICmp(CmpInst::ICMP_UGT, scalarValidTo,
                    builder.getInt64(snapshot->baseVersion()));

            auto scalarResult = builder.CreateAnd(scalarValidFrom, scalarValidTo);

            // Evaluate partitioning key % partitionModulo == partitionNumber
            if (state.partitionModulo != 0u) {
                auto scalarKey = builder.CreateInBoundsGEP(args[keyData], scalarLoopCounter);
                scalarKey = builder.CreateAlignedLoad(scalarKey, 8u);

                auto scalarComp = builder.CreateICmp(CmpInst::ICMP_EQ,
                        builder.createConstMod(scalarKey, state.partitionModulo),
                        builder.getInt64(state.partitionNumber));
                scalarResult = builder.CreateAnd(scalarResult, scalarComp);
            }
            scalarResult = builder.CreateZExtOrBitCast(scalarResult, builder.getInt8Ty());

            Value* scalarConjunct = scalarLoopCounter;
            if (i > 0) {
                scalarConjunct = builder.CreateAdd(scalarConjunct, builder.createConstMul(args[count], i));
            }
            scalarConjunct = builder.CreateInBoundsGEP(args[resultData], scalarConjunct);
            builder.CreateAlignedStore(scalarResult, scalarConjunct, 1u);

            // Vector -> validFrom <= version && validTo > baseVersion
            builder.SetInsertPoint(vectorBodyBlock);
            auto vectorValidFrom = builder.CreateInBoundsGEP(args[validFromData], vectorLoopCounter);
            vectorValidFrom = builder.CreateBitCast(vectorValidFrom, builder.getInt64VectorPtrTy(vectorSize));
            vectorValidFrom = builder.CreateAlignedLoad(vectorValidFrom, 8u);
            vectorValidFrom = builder.CreateICmp(CmpInst::ICMP_ULE, vectorValidFrom,
                    builder.getInt64Vector(vectorSize, snapshot->version()));

            auto vectorValidTo = builder.CreateInBoundsGEP(args[validToData], vectorLoopCounter);
            vectorValidTo = builder.CreateBitCast(vectorValidTo, builder.getInt64VectorPtrTy(vectorSize));
            vectorValidTo = builder.CreateAlignedLoad(vectorValidTo, 8u);
            vectorValidTo = builder.CreateICmp(CmpInst::ICMP_UGT, vectorValidTo,
                    builder.getInt64Vector(vectorSize, snapshot->baseVersion()));

            auto vectorResult = builder.CreateAnd(vectorValidFrom, vectorValidTo);

            // Evaluate partitioning key % partitionModulo == partitionNumber
            if (state.partitionModulo != 0u) {
                auto vectorKey = builder.CreateInBoundsGEP(args[keyData], vectorLoopCounter);
                vectorKey = builder.CreateBitCast(vectorKey, builder.getInt64VectorPtrTy(vectorSize));
                vectorKey = builder.CreateAlignedLoad(vectorKey, 8u);
                auto vectorComp = builder.CreateICmp(CmpInst::ICMP_EQ,
                        builder.createConstMod(vectorKey, state.partitionModulo, vectorSize),
                        builder.getInt64Vector(vectorSize, state.partitionNumber));
                vectorResult = builder.CreateAnd(vectorResult, vectorComp);
            }
            vectorResult = builder.CreateZExtOrBitCast(vectorResult, builder.getInt8VectorTy(vectorSize));

            Value* vectorConjunct = vectorLoopCounter;
            if (i > 0) {
                vectorConjunct = builder.CreateAdd(vectorConjunct, builder.createConstMul(args[count], i));
            }
            vectorConjunct = builder.CreateInBoundsGEP(args[resultData], vectorConjunct);
            vectorConjunct = builder.CreateBitCast(vectorConjunct, builder.getInt8VectorPtrTy(vectorSize));
            builder.CreateAlignedStore(vectorResult, vectorConjunct, 1u);
        }

        // -> i += 1;
        builder.SetInsertPoint(scalarBodyBlock);
        auto scalarNextVar = builder.CreateAdd(scalarLoopCounter, builder.getInt64(1));
        scalarLoopCounter->addIncoming(scalarNextVar, scalarBodyBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        auto vectorNextVar = builder.CreateAdd(vectorLoopCounter, builder.getInt64(vectorSize));
        vectorLoopCounter->addIncoming(vectorNextVar, vectorBodyBlock);

        // i != endIdx
        builder.SetInsertPoint(scalarBodyBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, scalarNextVar, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorNextVar, vectorEndIdx),
                vectorBodyBlock, vectorEndBlock);

        builder.SetInsertPoint(vectorEndBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorEndIdx, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        builder.SetInsertPoint(scalarEndBlock);
    }

    auto vectorSize = static_cast<uint64_t>(targetInfo.getRegisterBitWidth(true)) / 8;

    auto vectorCount = builder.CreateSub(args[endIdx], args[startIdx]);
    vectorCount = builder.CreateAnd(vectorCount, builder.getInt64(-vectorSize));
    auto vectorEndIdx = builder.CreateAdd(args[startIdx], vectorCount);

    for (decltype(queryBuffers.size()) i = 0; i < queryBuffers.size(); ++i) {
        auto& state = queryBuffers.at(i);
        if (state.numConjunct == 0u) {
            continue;
        }

        for (decltype(state.numConjunct) j = state.numConjunct - 1u; j > 0u; --j) {
            auto conjunctPos = state.conjunctOffset + j - 1u;

            auto previousBlock = builder.GetInsertBlock();
            auto vectorBodyBlock = BasicBlock::Create(mCompilerContext, "conj.vectorbody." + Twine(conjunctPos), func);
            auto vectorEndBlock = BasicBlock::Create(mCompilerContext, "conj.vectorend." + Twine(conjunctPos), func);
            auto scalarBodyBlock = BasicBlock::Create(mCompilerContext, "conj.scalarbody." + Twine(conjunctPos), func);
            auto scalarEndBlock = BasicBlock::Create(mCompilerContext, "conj.scalarend." + Twine(conjunctPos), func);
            builder.CreateCondBr(builder.CreateICmp(CmpInst::ICMP_NE, vectorCount, builder.getInt64(0)),
                    vectorBodyBlock, vectorEndBlock);

            // -> auto i = startIdx;
            builder.SetInsertPoint(scalarBodyBlock);
            auto scalarLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
            scalarLoopCounter->addIncoming(vectorEndIdx, vectorEndBlock);

            builder.SetInsertPoint(vectorBodyBlock);
            auto vectorLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
            vectorLoopCounter->addIncoming(args[startIdx], previousBlock);

            // Scalar
            builder.SetInsertPoint(scalarBodyBlock);
            auto scalarConjunctA = builder.CreateAdd(
                    builder.createConstMul(args[count], conjunctPos),
                    scalarLoopCounter);
            scalarConjunctA = builder.CreateInBoundsGEP(args[resultData], scalarConjunctA);
            auto scalarConjunctAValue = builder.CreateAlignedLoad(scalarConjunctA, 1u);

            auto scalarConjunctB = builder.CreateAdd(
                    builder.createConstMul(args[count], conjunctPos + 1),
                    scalarLoopCounter);
            scalarConjunctB = builder.CreateInBoundsGEP(args[resultData], scalarConjunctB);
            auto scalarConjunctBValue = builder.CreateAlignedLoad(scalarConjunctB, 1u);

            auto scalarResult = builder.CreateAnd(scalarConjunctAValue, scalarConjunctBValue);
            builder.CreateAlignedStore(scalarResult, scalarConjunctA, 1u);

            // Vector
            builder.SetInsertPoint(vectorBodyBlock);
            auto vectorConjunctA = builder.CreateAdd(
                    builder.createConstMul(args[count], conjunctPos),
                    vectorLoopCounter);
            vectorConjunctA = builder.CreateInBoundsGEP(args[resultData], vectorConjunctA);
            vectorConjunctA = builder.CreateBitCast(vectorConjunctA, builder.getInt8VectorPtrTy(vectorSize));
            auto vectorConjunctAValue = builder.CreateAlignedLoad(vectorConjunctA, 1u);

            auto vectorConjunctB = builder.CreateAdd(
                    builder.createConstMul(args[count], conjunctPos + 1),
                    vectorLoopCounter);
            vectorConjunctB = builder.CreateInBoundsGEP(args[resultData], vectorConjunctB);
            vectorConjunctB = builder.CreateBitCast(vectorConjunctB, builder.getInt8VectorPtrTy(vectorSize));
            auto vectorConjunctBValue = builder.CreateAlignedLoad(vectorConjunctB, 1u);

            auto vectorResult = builder.CreateAnd(vectorConjunctAValue, vectorConjunctBValue);
            builder.CreateAlignedStore(vectorResult, vectorConjunctA, 1u);

            // -> i += 1;
            builder.SetInsertPoint(scalarBodyBlock);
            auto scalarNextVar = builder.CreateAdd(scalarLoopCounter, builder.getInt64(1));
            scalarLoopCounter->addIncoming(scalarNextVar, scalarBodyBlock);

            builder.SetInsertPoint(vectorBodyBlock);
            auto vectorNextVar = builder.CreateAdd(vectorLoopCounter, builder.getInt64(vectorSize));
            vectorLoopCounter->addIncoming(vectorNextVar, vectorBodyBlock);

            // i != endIdx
            builder.SetInsertPoint(scalarBodyBlock);
            builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, scalarNextVar, args[endIdx]),
                    scalarBodyBlock, scalarEndBlock);

            builder.SetInsertPoint(vectorBodyBlock);
            builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorNextVar, vectorEndIdx),
                    vectorBodyBlock, vectorEndBlock);

            builder.SetInsertPoint(vectorEndBlock);
            builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorEndIdx, args[endIdx]),
                    scalarBodyBlock, scalarEndBlock);

            builder.SetInsertPoint(scalarEndBlock);
        }

        auto previousBlock = builder.GetInsertBlock();
        auto vectorBodyBlock = BasicBlock::Create(mCompilerContext, "conj.vectorbody." + Twine(i), func);
        auto vectorEndBlock = BasicBlock::Create(mCompilerContext, "conj.vectorend." + Twine(i), func);
        auto scalarBodyBlock = BasicBlock::Create(mCompilerContext, "conj.scalarbody." + Twine(i), func);
        auto scalarEndBlock = BasicBlock::Create(mCompilerContext, "conj.scalarend." + Twine(i), func);
        builder.CreateCondBr(builder.CreateICmp(CmpInst::ICMP_NE, vectorCount, builder.getInt64(0)),
                vectorBodyBlock, vectorEndBlock);

        // -> auto i = startIdx;
        builder.SetInsertPoint(scalarBodyBlock);
        auto scalarLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
        scalarLoopCounter->addIncoming(vectorEndIdx, vectorEndBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        auto vectorLoopCounter = builder.CreatePHI(builder.getInt64Ty(), 2);
        vectorLoopCounter->addIncoming(args[startIdx], previousBlock);

        // Scalar
        builder.SetInsertPoint(scalarBodyBlock);
        Value* scalarConjunctA = scalarLoopCounter;
        if (i > 0) {
            scalarConjunctA = builder.CreateAdd(scalarConjunctA, builder.createConstMul(args[count], i));
        }
        scalarConjunctA = builder.CreateInBoundsGEP(args[resultData], scalarConjunctA);
        auto scalarConjunctAValue = builder.CreateAlignedLoad(scalarConjunctA, 1u);

        auto scalarConjunctB = builder.CreateAdd(
                builder.createConstMul(args[count], state.conjunctOffset),
                scalarLoopCounter);
        scalarConjunctB = builder.CreateInBoundsGEP(args[resultData], scalarConjunctB);
        auto scalarConjunctBValue = builder.CreateAlignedLoad(scalarConjunctB, 1u);

        auto scalarResult = builder.CreateAnd(scalarConjunctAValue, scalarConjunctBValue);
        builder.CreateAlignedStore(scalarResult, scalarConjunctA, 1u);

        // Vector
        builder.SetInsertPoint(vectorBodyBlock);
        Value* vectorConjunctA = vectorLoopCounter;
        if (i > 0) {
            vectorConjunctA = builder.CreateAdd(vectorConjunctA, builder.createConstMul(args[count], i));
        }
        vectorConjunctA = builder.CreateInBoundsGEP(args[resultData], vectorConjunctA);
        vectorConjunctA = builder.CreateBitCast(vectorConjunctA, builder.getInt8VectorPtrTy(vectorSize));
        auto vectorConjunctAValue = builder.CreateAlignedLoad(vectorConjunctA, 1u);

        auto vectorConjunctB = builder.CreateAdd(
                builder.createConstMul(args[count], state.conjunctOffset),
                vectorLoopCounter);
        vectorConjunctB = builder.CreateInBoundsGEP(args[resultData], vectorConjunctB);
        vectorConjunctB = builder.CreateBitCast(vectorConjunctB, builder.getInt8VectorPtrTy(vectorSize));
        auto vectorConjunctBValue = builder.CreateAlignedLoad(vectorConjunctB, 1u);

        auto vectorResult = builder.CreateAnd(vectorConjunctAValue, vectorConjunctBValue);
        builder.CreateAlignedStore(vectorResult, vectorConjunctA, 1u);

        // -> i += 1;
        builder.SetInsertPoint(scalarBodyBlock);
        auto scalarNextVar = builder.CreateAdd(scalarLoopCounter, builder.getInt64(1));
        scalarLoopCounter->addIncoming(scalarNextVar, scalarBodyBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        auto vectorNextVar = builder.CreateAdd(vectorLoopCounter, builder.getInt64(vectorSize));
        vectorLoopCounter->addIncoming(vectorNextVar, vectorBodyBlock);

        // i != endIdx
        builder.SetInsertPoint(scalarBodyBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, scalarNextVar, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        builder.SetInsertPoint(vectorBodyBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorNextVar, vectorEndIdx),
                vectorBodyBlock, vectorEndBlock);

        builder.SetInsertPoint(vectorEndBlock);
        builder.CreateCondBr(builder.CreateICmp(ICmpInst::ICMP_NE, vectorEndIdx, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        builder.SetInsertPoint(scalarEndBlock);
    }

    // Return
    builder.CreateRetVoid();
}

void ColumnMapScan::prepareColumnProjectionFunction(const Record& srcRecord, ScanQuery* query, uint32_t index) {
    using namespace llvm;

    static constexpr size_t recordData = 0;
    static constexpr size_t heapData = 1;
    static constexpr size_t count = 2;
    static constexpr size_t idx = 3;
    static constexpr size_t destData = 4;

    LLVMBuilder builder(mCompilerContext);

    // Create function
    auto funcType = FunctionType::get(builder.getInt32Ty(), {
            builder.getInt8PtrTy(), // recordData
            builder.getInt8PtrTy(), // heapData
            builder.getInt64Ty(),   // count
            builder.getInt64Ty(),   // idx
            builder.getInt8PtrTy()  // destData
    }, false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gColumnMaterializeFunctionName + Twine(index),
            &mCompilerModule);

    // Set arguments names
    std::array<Value*, 5> args;
    {
        decltype(gColumnProjectionParamNames.size()) idx = 0;
        for (auto iter = func->arg_begin(); idx != gColumnProjectionParamNames.size(); ++iter, ++idx) {
            iter->setName(gColumnProjectionParamNames[idx]);
            args[idx] = iter.operator ->();
        }
    }

    // Set noalias hints (data pointers are not allowed to overlap)
    func->setDoesNotAlias(1);
    func->setOnlyReadsMemory(1);
    func->setDoesNotAlias(2);
    func->setOnlyReadsMemory(2);
    func->setDoesNotAlias(5);

    // Build function
    auto bb = BasicBlock::Create(mCompilerContext, "entry", func);
    builder.SetInsertPoint(bb);

    if (query->headerLength() != 0u) {
        builder.CreateMemSet(args[destData], builder.getInt8(0), query->headerLength(), 8u);
    }

    auto& destRecord = query->record();
    Record::id_t destFieldIdx = 0u;
    auto end = query->projectionEnd();
    for (auto i = query->projectionBegin(); i != end; ++i, ++destFieldIdx) {
        auto srcFieldIdx = *i;
        auto& srcFieldMeta = srcRecord.getFieldMeta(srcFieldIdx);
        auto& field = srcFieldMeta.first;

        if (!field.isNotNull()) {
            auto srcIdx = srcFieldIdx / 8u;
            auto srcBitIdx = srcFieldIdx % 8u;
            uint8_t srcMask = (0x1u << srcBitIdx);

            auto srcNullBitmap = builder.CreateInBoundsGEP(
                    args[recordData],
                    builder.createConstMul(args[idx], query->headerLength()));
            if (srcIdx != 0) {
                srcNullBitmap = builder.CreateInBoundsGEP(srcNullBitmap, builder.getInt64(srcIdx));
            }
            srcNullBitmap = builder.CreateAnd(builder.CreateAlignedLoad(srcNullBitmap, 1u), builder.getInt8(srcMask));

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

            auto res = builder.CreateOr(builder.CreateAlignedLoad(destNullBitmap, 1u), srcNullBitmap);
            builder.CreateAlignedStore(res, destNullBitmap, 1u);
        }

        auto srcFieldOffset = srcFieldMeta.second;
        auto destFieldOffset = destRecord.getFieldMeta(destFieldIdx).second;
        LOG_ASSERT(srcFieldOffset >= 0 && destFieldOffset >= 0, "Only fixed size supported at the moment");

        auto fieldAlignment = field.alignOf();
        auto fieldPtrType = builder.getFieldPtrTy(field.type());
        auto src = (srcFieldOffset == 0
                ? args[recordData]
                : builder.CreateInBoundsGEP(args[recordData], builder.createConstMul(args[count], srcFieldOffset)));
        src = builder.CreateBitCast(src, fieldPtrType);
        src = builder.CreateInBoundsGEP(src, args[idx]);

        auto dest = (destFieldOffset == 0
                ? args[destData]
                : builder.CreateInBoundsGEP(args[destData], builder.getInt64(destFieldOffset)));
        dest = builder.CreateBitCast(dest, fieldPtrType);
        builder.CreateAlignedStore(builder.CreateAlignedLoad(src, fieldAlignment), dest, fieldAlignment);
    }

    builder.CreateRet(builder.getInt32(destRecord.variableSizeOffset()));
}

void ColumnMapScan::prepareColumnAggregationFunction(const Record& srcRecord, ScanQuery* query, uint32_t index) {
    using namespace llvm;

    static constexpr size_t recordData = 0;
    static constexpr size_t heapData = 1;
    static constexpr size_t count = 2;
    static constexpr size_t startIdx = 3;
    static constexpr size_t endIdx = 4;
    static constexpr size_t resultData = 5;
    static constexpr size_t destData = 6;

    LLVMBuilder builder(mCompilerContext);

    // Create function
    auto funcType = FunctionType::get(builder.getInt32Ty(), {
            builder.getInt8PtrTy(), // recordData
            builder.getInt8PtrTy(), // heapData
            builder.getInt64Ty(),   // count
            builder.getInt64Ty(),   // startIdx
            builder.getInt64Ty(),   // endIdx
            builder.getInt8PtrTy(), // resultData
            builder.getInt8PtrTy()  // destData
    }, false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gColumnMaterializeFunctionName + Twine(index),
            &mCompilerModule);

    auto targetInfo = mCompiler.getTargetMachine()->getTargetIRAnalysis().run(*func);

    // Set arguments names
    std::array<Value*, 7> args;
    {
        decltype(gColumnAggregationParamNames.size()) idx = 0;
        for (auto iter = func->arg_begin(); idx != gColumnAggregationParamNames.size(); ++iter, ++idx) {
            iter->setName(gColumnAggregationParamNames[idx]);
            args[idx] = iter.operator ->();
        }
    }

    // Set noalias hints (data pointers are not allowed to overlap)
    func->setDoesNotAlias(1);
    func->setOnlyReadsMemory(1);
    func->setDoesNotAlias(2);
    func->setOnlyReadsMemory(2);
    func->setDoesNotAlias(6);
    func->setOnlyReadsMemory(6);
    func->setDoesNotAlias(7);

    // Build function
    auto entryBlock = BasicBlock::Create(mCompilerContext, "entry", func);
    builder.SetInsertPoint(entryBlock);

    auto& destRecord = query->record();
    Record::id_t j = 0u;
    auto end = query->aggregationEnd();
    for (auto i = query->aggregationBegin(); i != end; ++i, ++j) {
        uint16_t srcFieldIdx;
        AggregationType aggregationType;
        std::tie(srcFieldIdx, aggregationType) = *i;
        auto& srcFieldMeta = srcRecord.getFieldMeta(srcFieldIdx);
        auto& srcField = srcFieldMeta.first;
        auto srcFieldAlignment = srcField.alignOf();
        auto srcFieldPtrType = builder.getFieldPtrTy(srcField.type());
        auto srcFieldOffset = srcFieldMeta.second;

        uint16_t destFieldIdx;
        destRecord.idOf(crossbow::to_string(j), destFieldIdx);
        auto& destFieldMeta = destRecord.getFieldMeta(destFieldIdx);
        auto& destField = destFieldMeta.first;
        auto destFieldAlignment = destField.alignOf();
        auto destFieldSize = destField.staticSize();
        auto destFieldType = builder.getFieldTy(destField.type());
        auto vectorSize = static_cast<uint64_t>(targetInfo.getRegisterBitWidth(true)) / (destFieldSize * 8);
        auto destFieldVectorType = VectorType::get(destFieldType, vectorSize);
        auto destFieldPtrType = builder.getFieldPtrTy(destField.type());
        auto destFieldOffset = destFieldMeta.second;
        LOG_ASSERT(srcFieldOffset >= 0 && destFieldOffset >= 0, "Only fixed size supported at the moment");

        auto isFloat = (srcField.type() == FieldType::FLOAT) || (srcField.type() == FieldType::DOUBLE);

        // Aggregation function used by the vector and scalar code paths
        auto builderAggregation = [&builder, &srcField, isFloat, aggregationType]
                (Value* src, Value* dest, Value* result) {
            switch (aggregationType) {
            case AggregationType::MIN: {
                auto cond = (isFloat
                        ? builder.CreateFCmp(CmpInst::FCMP_OLT, src, dest)
                        : builder.CreateICmp(CmpInst::ICMP_SLT, src, dest));
                cond = builder.CreateAnd(result, cond);
                return builder.CreateSelect(cond, src, dest);
            } break;

            case AggregationType::MAX: {
                auto cond = (isFloat
                        ? builder.CreateFCmp(CmpInst::FCMP_OGT, src, dest)
                        : builder.CreateICmp(CmpInst::ICMP_SGT, src, dest));
                cond = builder.CreateAnd(result, cond);
                return builder.CreateSelect(cond, src, dest);
            } break;

            case AggregationType::SUM: {
                if (srcField.type() == FieldType::SMALLINT || srcField.type() == FieldType::INT) {
                    src = builder.CreateSExt(src, dest->getType());
                } else if (srcField.type() == FieldType::FLOAT) {
                    src = builder.CreateFPExt(src, dest->getType());
                }

                auto res = (isFloat
                        ? builder.CreateFAdd(dest, src)
                        : builder.CreateAdd(dest, src));
                return builder.CreateSelect(result, res, dest);
            } break;

            case AggregationType::CNT: {
                return builder.CreateAdd(dest, builder.CreateZExt(result, dest->getType()));
            } break;

            default: {
                LOG_ASSERT(false, "Unknown aggregation type");
                return static_cast<Value*>(nullptr);
            } break;
            }
        };

        // Create code blocks
        auto previousBlock = builder.GetInsertBlock();
        auto vectorHeaderBlock = BasicBlock::Create(mCompilerContext, "agg.vectorheader." + Twine(destFieldIdx), func);
        auto vectorBodyBlock = BasicBlock::Create(mCompilerContext, "agg.vectorbody." + Twine(destFieldIdx), func);
        auto vectorMergeBlock = BasicBlock::Create(mCompilerContext, "agg.vectormerge." + Twine(destFieldIdx), func);
        auto vectorEndBlock = BasicBlock::Create(mCompilerContext, "agg.vectorend." + Twine(destFieldIdx), func);
        auto scalarBodyBlock = BasicBlock::Create(mCompilerContext, "agg.scalarbody." + Twine(destFieldIdx), func);
        auto scalarEndBlock = BasicBlock::Create(mCompilerContext, "agg.scalarend." + Twine(destFieldIdx), func);

        // Compute the pointer to the first element in the aggregation column
        Value* srcPtr;
        if (aggregationType != AggregationType::CNT) {
            srcPtr = (srcFieldOffset == 0
                    ? args[recordData]
                    : builder.CreateInBoundsGEP(args[recordData], builder.createConstMul(args[count], srcFieldOffset)));
            srcPtr = builder.CreateBitCast(srcPtr, srcFieldPtrType);
        }

        // Load the aggregation value from the previous run
        auto destPtr = (destFieldOffset == 0
                ? args[destData]
                : builder.CreateInBoundsGEP(args[destData], builder.getInt64(destFieldOffset)));
        destPtr = builder.CreateBitCast(destPtr, destFieldPtrType);
        auto destValue = builder.CreateAlignedLoad(destPtr, destFieldAlignment);

        // Check how many vector iterations can be executed
        // Skip to the vector end if no vectorized iterations can be executed
        auto vectorCount = builder.CreateSub(args[endIdx], args[startIdx]);
        vectorCount = builder.CreateAnd(vectorCount, builder.getInt64(-vectorSize));
        auto vectorEndIdx = builder.CreateAdd(args[startIdx], vectorCount);
        builder.CreateCondBr(
                builder.CreateICmp(CmpInst::ICMP_NE, vectorCount, builder.getInt64(0)),
                vectorHeaderBlock, vectorEndBlock);

        // Vector header
        // Initialize the start vector: In case of min/max the current min/max element is broadcasted to the complete
        // vector, for sum and count the current sum/cnt value is stored in the first element and the remaining vector
        // filled with zeroes.
        builder.SetInsertPoint(vectorHeaderBlock);
        Value* vectorDestValue;
        switch (aggregationType) {
        case AggregationType::MIN:
        case AggregationType::MAX: {
            vectorDestValue = builder.CreateVectorSplat(vectorSize, destValue);
        } break;

        case AggregationType::SUM: {
            vectorDestValue = isFloat
                    ? builder.getDoubleVector(vectorSize, 0)
                    : builder.getInt64Vector(vectorSize, 0);
            vectorDestValue = builder.CreateInsertElement(vectorDestValue, destValue, static_cast<uint64_t>(0));
        } break;

        case AggregationType::CNT: {
            vectorDestValue = builder.getInt64Vector(vectorSize, 0);
            vectorDestValue = builder.CreateInsertElement(vectorDestValue, destValue, static_cast<uint64_t>(0));
        } break;

        default: {
            LOG_ASSERT(false, "Unknown aggregation type");
            vectorDestValue = nullptr;
        } break;
        }
        builder.CreateBr(vectorBodyBlock);

        // Vector Body
        // Contains the aggregation loop
        builder.SetInsertPoint(vectorBodyBlock);
        auto vectorIdx = builder.CreatePHI(builder.getInt64Ty(), 2);
        vectorIdx->addIncoming(args[startIdx], vectorHeaderBlock);

        // Create PHI node containing the immediate result in the loop
        auto vectorDest = builder.CreatePHI(destFieldVectorType, 2);
        vectorDest->addIncoming(vectorDestValue, vectorHeaderBlock);

        // Load source vector (not required for count aggregation)
        Value* vectorSrc;
        if (aggregationType != AggregationType::CNT) {
            vectorSrc = builder.CreateInBoundsGEP(srcPtr, vectorIdx);
            vectorSrc = builder.CreateBitCast(vectorSrc, builder.getFieldVectorPtrTy(vectorSize, srcField.type()));
            vectorSrc = builder.CreateAlignedLoad(vectorSrc, srcFieldAlignment);
        }

        // Load result vector
        auto vectorResult = builder.CreateInBoundsGEP(args[resultData], vectorIdx);
        vectorResult = builder.CreateBitCast(vectorResult, builder.getInt8VectorPtrTy(vectorSize));
        vectorResult = builder.CreateAlignedLoad(vectorResult, 1u);
        vectorResult = builder.CreateTruncOrBitCast(vectorResult, builder.getInt1VectorTy(vectorSize));

        // Evaluate aggregation
        auto vectorAgg = builderAggregation(vectorSrc, vectorDest, vectorResult);
        vectorDest->addIncoming(vectorAgg, vectorBodyBlock);

        // Advance the loop
        auto vectorNextIdx = builder.CreateAdd(vectorIdx, builder.getInt64(vectorSize));
        vectorIdx->addIncoming(vectorNextIdx, vectorBodyBlock);
        builder.CreateCondBr(
                builder.CreateICmp(ICmpInst::ICMP_NE, vectorNextIdx, vectorEndIdx),
                vectorBodyBlock, vectorMergeBlock);

        // Vector Merge
        // Reduce the individual aggregations in the vector to one value by recursively aggregating the upper with the
        // lower values in the vector until only one value is left.
        builder.SetInsertPoint(vectorMergeBlock);
        std::vector<Constant*> reduceIdx;
        reduceIdx.reserve(vectorSize);
        for (auto i = vectorSize; i > 1; i /= 2) {
            for (auto j = i / 2; j < i; ++j) {
                reduceIdx.emplace_back(builder.getInt32(j));
            }
            for (auto j = i / 2; j < vectorSize; ++j) {
                reduceIdx.emplace_back(UndefValue::get(builder.getInt32Ty()));
            }
            auto reduce = builder.CreateShuffleVector(vectorAgg, UndefValue::get(destFieldVectorType),
                    ConstantVector::get(reduceIdx));

            switch (aggregationType) {
            case AggregationType::MIN: {
                auto cond = (isFloat
                        ? builder.CreateFCmp(CmpInst::FCMP_OLT, vectorAgg, reduce)
                        : builder.CreateICmp(CmpInst::ICMP_SLT, vectorAgg, reduce));
                vectorAgg = builder.CreateSelect(cond, vectorAgg, reduce);
            } break;

            case AggregationType::MAX: {
                auto cond = (isFloat
                        ? builder.CreateFCmp(CmpInst::FCMP_OGT, vectorAgg, reduce)
                        : builder.CreateICmp(CmpInst::ICMP_SGT, vectorAgg, reduce));
                vectorAgg = builder.CreateSelect(cond, vectorAgg, reduce);
            } break;

            case AggregationType::SUM: {
                vectorAgg = (isFloat
                        ? builder.CreateFAdd(vectorAgg, reduce)
                        : builder.CreateAdd(vectorAgg, reduce));
            } break;

            case AggregationType::CNT: {
                vectorAgg = builder.CreateAdd(vectorAgg, reduce);
            } break;

            default: {
                LOG_ASSERT(false, "Unknown aggregation type");
            } break;
            }
            reduceIdx.clear();
        }
        vectorAgg = builder.CreateExtractElement(vectorAgg, static_cast<uint64_t>(0));
        builder.CreateBr(vectorEndBlock);

        // Vector end block
        // Merge result from vectorized code or the result from the previous run if no vector iterations were executed.
        // Branch to scalar code if additional scalar iterations are required.
        builder.SetInsertPoint(vectorEndBlock);
        auto vectorAggResult = builder.CreatePHI(destFieldType, 2);
        vectorAggResult->addIncoming(destValue, previousBlock);
        vectorAggResult->addIncoming(vectorAgg, vectorMergeBlock);
        builder.CreateCondBr(
                builder.CreateICmp(ICmpInst::ICMP_NE, vectorEndIdx, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        // Scalar Body
        // Contains the aggregation loop
        builder.SetInsertPoint(scalarBodyBlock);
        auto scalarIdx = builder.CreatePHI(builder.getInt64Ty(), 2);
        scalarIdx->addIncoming(vectorEndIdx, vectorEndBlock);

        // Create PHI node containing the immediate result in the loop
        auto scalarDest = builder.CreatePHI(destFieldType, 2);
        scalarDest->addIncoming(vectorAggResult, vectorEndBlock);

        // Load source vector (not required for count aggregation)
        Value* scalarSrc;
        if (aggregationType != AggregationType::CNT) {
            scalarSrc = builder.CreateInBoundsGEP(srcPtr, scalarIdx);
            scalarSrc = builder.CreateAlignedLoad(scalarSrc, srcFieldAlignment);
        }

        // Load result vector
        auto scalarResult = builder.CreateInBoundsGEP(args[resultData], scalarIdx);
        scalarResult = builder.CreateAlignedLoad(scalarResult, 1u);
        scalarResult = builder.CreateTruncOrBitCast(scalarResult, builder.getInt1Ty());

        // Evaluate aggregation
        auto scalarAgg = builderAggregation(scalarSrc, scalarDest, scalarResult);
        scalarDest->addIncoming(scalarAgg, scalarBodyBlock);

        // Advance the loop
        auto scalarNextIdx = builder.CreateAdd(scalarIdx, builder.getInt64(1));
        scalarIdx->addIncoming(scalarNextIdx, scalarBodyBlock);
        builder.CreateCondBr(
                builder.CreateICmp(ICmpInst::ICMP_NE, scalarNextIdx, args[endIdx]),
                scalarBodyBlock, scalarEndBlock);

        // Scalar end block
        builder.SetInsertPoint(scalarEndBlock);
        auto aggResult = builder.CreatePHI(destFieldType, 2);
        aggResult->addIncoming(vectorAggResult, vectorEndBlock);
        aggResult->addIncoming(scalarAgg, scalarBodyBlock);
        builder.CreateAlignedStore(aggResult, destPtr, destFieldAlignment);
    }

    builder.CreateRet(builder.getInt32(destRecord.variableSizeOffset()));
}

ColumnMapScanProcessor::ColumnMapScanProcessor(const ColumnMapContext& context, const Record& record,
        const std::vector<ScanQuery*>& queries, const PageList& pages, size_t pageIdx, size_t pageEndIdx,
        const LogIterator& logIter, const LogIterator& logEnd, ColumnMapScan::ColumnScanFun columnScanFun,
        const std::vector<ColumnMapScan::ColumnProjectionFun>& columnProjectionFuns,
        const std::vector<ColumnMapScan::ColumnAggregationFun>& columnAggregationFuns,
        ColumnMapScan::RowScanFun rowScanFun, const std::vector<ColumnMapScan::RowMaterializeFun>& rowMaterializeFuns,
        uint32_t numConjuncts)
        : LLVMRowScanProcessorBase(record, queries, rowScanFun, rowMaterializeFuns, numConjuncts),
          mContext(context),
          mColumnScanFun(columnScanFun),
          mColumnProjectionFuns(columnProjectionFuns),
          mColumnAggregationFuns(columnAggregationFuns),
          pages(pages),
          pageIdx(pageIdx),
          pageEndIdx(pageEndIdx),
          logIter(logIter),
          logEnd(logEnd) {
}

void ColumnMapScanProcessor::process() {
    for (auto i = pageIdx; i < pageEndIdx; ++i) {
        processMainPage(pages[i], 0, pages[i]->count);
    }

    auto insIter = logIter;
    while (insIter != logEnd) {
        if (!insIter->sealed()) {
            ++insIter;
            continue;
        }

        auto ptr = reinterpret_cast<const InsertLogEntry*>(insIter->data());
        ConstInsertRecord record(ptr);
        if (!record.valid()) {
            ++insIter;
            continue;
        }

        if (auto relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(record.newest()))) {
            auto relocatedPage = mContext.pageFromEntry(relocated);
            auto relocatedStartIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
            auto relocatedEndIdx = relocatedStartIdx;

            for (++insIter; insIter != logEnd; ++insIter) {
                if (!insIter->sealed()) {
                    continue;
                }

                ptr = reinterpret_cast<const InsertLogEntry*>(insIter->data());
                record = ConstInsertRecord(ptr);

                relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(record.newest()));
                if (relocated) {
                    if (mContext.pageFromEntry(relocated) != relocatedPage) {
                        break;
                    }
                    relocatedEndIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
                } else if (!record.valid()) {
                    continue;
                } else {
                    break;
                }
            }

            auto relocatedEntries = relocatedPage->entryData();
            auto key = relocatedEntries[relocatedEndIdx].key;
            for (++relocatedEndIdx; relocatedEndIdx < relocatedPage->count && relocatedEntries[relocatedEndIdx].key == key; ++relocatedEndIdx);

            processMainPage(relocatedPage, relocatedStartIdx, relocatedEndIdx);

            continue;
        }

        auto validTo = std::numeric_limits<uint64_t>::max();
        if (record.newest() != 0u) {
            auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(record.newest()),
                    record.baseVersion(), validTo);

            if (ptr->version >= lowestVersion) {
                ++insIter;
                continue;
            }
        }
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(ptr));
        processRowRecord(ptr->key, ptr->version, validTo, ptr->data(), entry->size() - sizeof(InsertLogEntry));
        ++insIter;
    }
}

void ColumnMapScanProcessor::processMainPage(const ColumnMapMainPage* page, uint64_t startIdx, uint64_t endIdx) {
    mKeyData.resize(page->count, 0u);
    mValidFromData.resize(page->count, 0u);
    mValidToData.resize(page->count, 0u);
    auto resultSize = mNumConjuncts * page->count;
    if (mResult.size() < resultSize) {
        mResult.resize(resultSize, 0u);
    }

    auto entries = page->entryData();

    auto i = startIdx;
    while (i < endIdx) {
        auto key = entries[i].key;
        auto newest = entries[i].newest.load();
        auto validTo = std::numeric_limits<uint64_t>::max();
        if (newest != 0u) {
            if ((newest & crossbow::to_underlying(NewestPointerTag::INVALID)) != 0x0u) {
                // Skip to element with next key
                auto j = i;
                for (++i; i < endIdx && entries[i].key == key; ++i);
                if (startIdx == j) {
                    startIdx = i;
                }
                continue;
            }
            if (auto relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(newest))) {
                if (i > startIdx) {
                    evaluateMainQueries(page, startIdx, i);
                }

                auto relocatedPage = mContext.pageFromEntry(relocated);
                auto relocatedStartIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
                auto relocatedEndIdx = relocatedStartIdx;

                while (true) {
                    for (++i; i < endIdx && entries[i].key == key; ++i);
                    if (i >= endIdx) {
                        break;
                    }

                    key = entries[i].key;
                    newest = entries[i].newest.load();

                    relocated = reinterpret_cast<const ColumnMapMainEntry*>(newestMainRecord(newest));
                    if (relocated) {
                        if (mContext.pageFromEntry(relocated) != relocatedPage) {
                            break;
                        }
                        relocatedEndIdx = ColumnMapContext::pageIndex(relocatedPage, relocated);
                    } else if ((newest & crossbow::to_underlying(NewestPointerTag::INVALID)) != 0x0u) {
                        continue;
                    } else {
                        break;
                    }
                }

                auto relocatedEntries = relocatedPage->entryData();
                key = relocatedEntries[relocatedEndIdx].key;
                for (++relocatedEndIdx; relocatedEndIdx < relocatedPage->count && relocatedEntries[relocatedEndIdx].key == key; ++relocatedEndIdx);

                processMainPage(relocatedPage, relocatedStartIdx, relocatedEndIdx);

                if (i >= endIdx) {
                    return;
                }

                mKeyData.resize(page->count, 0u);
                mValidFromData.resize(page->count, 0u);
                mValidToData.resize(page->count, 0u);

                startIdx = i;
                continue;
            }

            auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(newest),
                    entries[i].version, validTo);

            // Skip elements with version above lowest version and set the valid-to version to 0 to exclude them from
            // the query processing
            auto j = i;
            for (; i < endIdx && entries[i].key == key && entries[i].version >= lowestVersion; ++i);
            if (startIdx == j) {
                startIdx = i;
            }
        }

        // Set valid-to version for every element of the same key to the valid-from version of the previous
        for (; i < endIdx && entries[i].key == key; ++i) {
            mKeyData[i] = key;
            mValidFromData[i] = entries[i].version;
            mValidToData[i] = validTo;
            validTo = entries[i].version;
        }
    }
    if (startIdx < endIdx) {
        evaluateMainQueries(page, startIdx, endIdx);
    }
}

void ColumnMapScanProcessor::evaluateMainQueries(const ColumnMapMainPage* page, uint64_t startIdx, uint64_t endIdx) {
    LOG_ASSERT(mKeyData.size() == page->count, "Size of key array does not match the page size");
    LOG_ASSERT(mValidFromData.size() == page->count, "Size of valid-from array does not match the page size");
    LOG_ASSERT(mValidToData.size() == page->count, "Size of valid-to array does not match the page size");

    mColumnScanFun(&mKeyData.front(), &mValidFromData.front(), &mValidToData.front(), page->recordData(),
            page->heapData(), page->count, startIdx, endIdx, &mResult.front());

    auto entries = page->entryData();
    auto sizeData = page->sizeData();
    auto result = &mResult.front();
    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        switch (mQueries[i].data()->queryType()) {
        case ScanQueryType::FULL: {
            for (decltype(startIdx) j = startIdx; j < endIdx; ++j) {
                if (result[j] == 0u) {
                    continue;
                }
                auto length = sizeData[j];
                mQueries[i].writeRecord(entries[j].key, length, entries[j].version, mValidToData[j],
                        [this, page, j, length] (char* dest) {
                    mContext.materialize(page, j, dest, length);
                    return length;
                });
            }
        } break;

        case ScanQueryType::PROJECTION: {
            for (decltype(startIdx) j = startIdx; j < endIdx; ++j) {
                if (result[j] == 0u) {
                    continue;
                }
                auto length = sizeData[j];
                mQueries[i].writeRecord(entries[j].key, length, entries[j].version, mValidToData[j],
                        [this, page, i, j] (char* dest) {
                    return mColumnProjectionFuns[i](page->recordData(), page->heapData(), page->count, j, dest);
                });
            }
        } break;

        case ScanQueryType::AGGREGATION: {
            mColumnAggregationFuns[i](page->recordData(), page->heapData(), page->count, startIdx, endIdx, result,
                    mQueries[i].mBuffer + 8);
        } break;

        }
        result += page->count;
    }

    mKeyData.clear();
    mValidFromData.clear();
    mValidToData.clear();
}

uint64_t ColumnMapScanProcessor::processUpdateRecord(const UpdateLogEntry* ptr, uint64_t baseVersion,
        uint64_t& validTo) {
    UpdateRecordIterator updateIter(ptr, baseVersion);
    for (; !updateIter.done(); updateIter.next()) {
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(updateIter.value()));

        // Check if the entry marks a deletion: Skip element
        if (entry->type() == crossbow::to_underlying(RecordType::DELETE)) {
            validTo = updateIter->version;
            continue;
        }

        processRowRecord(updateIter->key, updateIter->version, validTo, updateIter->data(),
                entry->size() - sizeof(UpdateLogEntry));
        validTo = updateIter->version;
    }
    return updateIter.lowestVersion();
}

} // namespace deltamain
} // namespace store
} // namespace tell
