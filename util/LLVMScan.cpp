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

#include "LLVMBuilder.hpp"

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
#include <string>

namespace tell {
namespace store {
namespace {

static const std::string gRowScanFunctionName = "rowScan";

static const std::array<std::string, 5> gRowScanParamNames = {{
    "key",
    "validFrom",
    "validTo",
    "recordData",
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

LLVMScanBase::LLVMScanBase()
        :  mCompilerModule("ScanQuery", mCompilerContext) {
    mCompilerModule.setDataLayout(mCompiler.getTargetMachine()->createDataLayout());
    mCompilerModule.setTargetTriple(mCompiler.getTargetMachine()->getTargetTriple().getTriple());
}

void LLVMScanBase::finalizeScan() {
    using namespace llvm;

#ifndef NDEBUG
    LOG_INFO("Dumping LLVM Code before optimizations");
    mCompilerModule.dump();
#endif

    // Setup optimizations
    legacy::PassManager modulePass;
#ifndef NDEBUG
    modulePass.add(createVerifierPass());
#endif
    modulePass.add(createTargetTransformInfoWrapperPass(mCompiler.getTargetMachine()->getTargetIRAnalysis()));

    legacy::FunctionPassManager functionPass(&mCompilerModule);
#ifndef NDEBUG
    functionPass.add(createVerifierPass());
#endif
    functionPass.add(createTargetTransformInfoWrapperPass(mCompiler.getTargetMachine()->getTargetIRAnalysis()));

    PassManagerBuilder optimizationBuilder;
    optimizationBuilder.OptLevel = 3;
    optimizationBuilder.BBVectorize = true;
    optimizationBuilder.SLPVectorize = true;
    optimizationBuilder.LoopVectorize = true;
    optimizationBuilder.RerollLoops = true;
    optimizationBuilder.LoadCombine = true;
    optimizationBuilder.populateFunctionPassManager(functionPass);
    optimizationBuilder.populateModulePassManager(modulePass);
    optimizationBuilder.populateLTOPassManager(modulePass);

    functionPass.doInitialization();
    for (auto& func : mCompilerModule) {
        // Add host CPU features
        func.addFnAttr(Attribute::NoUnwind);
        func.addFnAttr("target-cpu", mCompiler.getTargetMachine()->getTargetCPU());
        func.addFnAttr("target-features", mCompiler.getTargetMachine()->getTargetFeatureString());

        // Run passes
        functionPass.run(func);
    }
    functionPass.doFinalization();

    modulePass.run(mCompilerModule);

#ifndef NDEBUG
    LOG_INFO("Dumping LLVM Code after optimizations");
    mCompilerModule.dump();
#endif

    // Compile the module
    mCompiler.addModule(&mCompilerModule);
}


LLVMRowScanBase::LLVMRowScanBase(const Record& record, std::vector<ScanQuery*> queries)
        : mQueries(std::move(queries)),
          mRowScanFun(nullptr),
          mNumConjuncts(0u) {
    prepareRowScanFunction(record);
}

void LLVMRowScanBase::finalizeRowScan() {
    LOG_ASSERT(!mRowScanFun, "Scan already finalized");
    finalizeScan();
    mRowScanFun = mCompiler.findFunction<RowScanFun>(gRowScanFunctionName);
}

void LLVMRowScanBase::prepareRowScanFunction(const Record &record) {
    using namespace llvm;

    static constexpr size_t key = 0;
    static constexpr size_t validFrom = 1;
    static constexpr size_t validTo = 2;
    static constexpr size_t recordData = 3;
    static constexpr size_t destData = 4;

    LLVMBuilder builder(mCompilerContext);

    // Create function
    auto funcType = FunctionType::get(builder.getVoidTy(), {
            builder.getInt64Ty(),   // key
            builder.getInt64Ty(),   // validFrom
            builder.getInt64Ty(),   // validTo
            builder.getInt8PtrTy(), // recordData
            builder.getInt8PtrTy()  // destData
    }, false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gRowScanFunctionName, &mCompilerModule);

    // Set arguments names
    std::array<Value*, 5> args;
    {
        decltype(gRowScanParamNames.size()) idx = 0;
        for (auto iter = func->arg_begin(); idx != gRowScanParamNames.size(); ++iter, ++idx) {
            iter->setName(gRowScanParamNames[idx]);
            args[idx] = iter.operator ->();
        }
    }

    // Set noalias hints (data pointers are not allowed to overlap)
    func->setDoesNotAlias(4);
    func->setOnlyReadsMemory(4);
    func->setDoesNotAlias(5);

    // Build function
    auto bb = BasicBlock::Create(mCompilerContext, "entry", func);
    builder.SetInsertPoint(bb);

    if (mQueries.size() == 0) {
        builder.CreateRetVoid();
        return;
    }

    std::vector<QueryState> queryBuffers;
    queryBuffers.reserve(mQueries.size());

    mNumConjuncts = mQueries.size();
    for (auto q : mQueries) {
        queryBuffers.emplace_back(q, mNumConjuncts);

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

        mNumConjuncts += state.numConjunct;
    }

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

        auto& field = record.getFieldMeta(currentColumn).first;

        Value* nullValue = nullptr;
        if (!field.isNotNull()) {
            auto idx = currentColumn / 8u;
            uint8_t mask = (0x1u << (currentColumn % 8u));

            auto nullBitmap = (idx == 0
                    ? args[recordData]
                    : builder.CreateInBoundsGEP(args[recordData], builder.getInt64(idx)));
            nullValue = builder.CreateAnd(builder.CreateLoad(nullBitmap), builder.getInt8(mask));
        }

        auto fieldOffset = record.getFieldMeta(currentColumn).second;
        auto fieldAlignment = field.alignOf();
        auto src = (fieldOffset == 0
                    ? args[recordData]
                    : builder.CreateInBoundsGEP(args[recordData], builder.getInt64(fieldOffset)));
        src = builder.CreateBitCast(src, builder.getFieldPtrTy(field.type()));
        src = builder.CreateAlignedLoad(src, fieldAlignment);

        // Process all queries with the next column
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

                Value* comp;
                switch (predicateType) {
                case PredicateType::IS_NULL:
                case PredicateType::IS_NOT_NULL: {
                    auto predicate = (predicateType == PredicateType::IS_NULL ? CmpInst::ICMP_NE : CmpInst::ICMP_EQ);
                    comp = builder.CreateICmp(predicate, nullValue, builder.getInt8(0));
                    queryReader.advance(6);
                } break;

                default: {
                    bool isFloat;
                    Value* compareValue;
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
                        LOG_ASSERT(false, "Unknown or invalid predicate");
                        predicate = (isFloat ? CmpInst::BAD_FCMP_PREDICATE : CmpInst::BAD_ICMP_PREDICATE);
                    } break;
                    }

                    comp = (isFloat
                            ? builder.CreateFCmp(predicate, src, compareValue)
                            : builder.CreateICmp(predicate, src, compareValue));

                    if (!field.isNotNull()) {
                        comp = builder.CreateAnd(
                                comp,
                                builder.CreateICmp(CmpInst::ICMP_EQ, nullValue, builder.getInt8(0)));
                    }
                } break;
                }

                comp = builder.CreateZExt(comp, builder.getInt8Ty());

                auto conjunctElement = builder.CreateInBoundsGEP(args[destData], builder.getInt64(conjunctPosition));

                // -> res[i] = res[i] | comp;
                auto res = builder.CreateOr(builder.CreateLoad(conjunctElement), comp);
                builder.CreateStore(res, conjunctElement);
            }
            if (queryReader.exhausted()) {
                state.currentColumn = std::numeric_limits<uint16_t>::max();
            } else {
                state.currentColumn = queryReader.read<uint16_t>();
            }
        }
        currentColumn = std::numeric_limits<uint16_t>::max();
    }

    for (decltype(queryBuffers.size()) i = 0; i < queryBuffers.size(); ++i) {
        auto& state = queryBuffers.at(i);
        auto snapshot = state.snapshot;

        // Evaluate validFrom <= version && validTo > baseVersion
        auto res = builder.CreateAnd(
                builder.CreateICmp(CmpInst::ICMP_ULE, args[validFrom], builder.getInt64(snapshot->version())),
                builder.CreateICmp(CmpInst::ICMP_UGT, args[validTo], builder.getInt64(snapshot->baseVersion())));

        // Evaluate partitioning key % partitionModulo == partitionNumber
        if (state.partitionModulo != 0u) {
            auto comp = builder.CreateICmp(CmpInst::ICMP_EQ,
                    builder.createConstMod(args[key], state.partitionModulo),
                    builder.getInt64(state.partitionNumber));
            res = builder.CreateAnd(res, comp);
        }
        res = builder.CreateZExt(res, builder.getInt8Ty());

        // Evaluate conjuncts
        for (decltype(state.numConjunct) j = 0; j < state.numConjunct; ++j) {
            auto conjunctBElement = builder.CreateInBoundsGEP(args[destData], builder.getInt64(state.conjunctOffset + j));
            res = builder.CreateAnd(res, builder.CreateLoad(conjunctBElement));
        }

        builder.CreateStore(res, builder.CreateInBoundsGEP(args[destData], builder.getInt64(i)));
    }

    // Return
    builder.CreateRetVoid();
}

LLVMRowScanProcessorBase::LLVMRowScanProcessorBase(const Record& record, const std::vector<ScanQuery*>& queries,
        LLVMRowScanBase::RowScanFun rowScanFunc, uint32_t numConjuncts)
        : mRecord(record),
          mRowScanFun(rowScanFunc),
          mNumConjuncts(numConjuncts) {
    LOG_ASSERT(mNumConjuncts >= queries.size(), "More queries than conjuncts");

    mQueries.reserve(queries.size());
    for (auto q : queries) {
        mQueries.emplace_back(q->createProcessor());
    }
}

void LLVMRowScanProcessorBase::processRowRecord(uint64_t key, uint64_t validFrom, uint64_t validTo, const char* data,
        uint32_t length) {
    mResult.resize(mNumConjuncts, 0);
    mRowScanFun(key, validFrom, validTo, data, &mResult.front());

    for (decltype(mQueries.size()) i = 0; i < mQueries.size(); ++i) {
        // Check if the selection string matches the record
        if (mResult[i] == 0) {
            continue;
        }
        mQueries[i].processRecord(mRecord, key, data, length, validFrom, validTo);
    }
    mResult.clear();
}

} // namespace store
} // namespace tell
