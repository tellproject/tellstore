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

#include "ColumnMapContext.hpp"

#include <tellstore/Record.hpp>

#include <util/LLVMBuilder.hpp>
#include <util/PageManager.hpp>

#include <crossbow/logger.hpp>

#include <llvm/ADT/ArrayRef.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>

#include <array>

namespace tell {
namespace store {
namespace deltamain {

namespace {

static const std::string gMaterializeFunctionName = "materialize";

static const std::array<std::string, 6> gMaterializeParamNames = {{
    "recordData",
    "heapData",
    "count",
    "idx",
    "dest",
    "size"
}};

/**
 * @brief Calculate the additional overhead required by every element in a column map page
 *
 * The overhead consists of the element's header, the size of the element and the heap entries for every variable sized
 * field.
 */
uint32_t calcEntryOverhead(const Record& record) {
    return sizeof(ColumnMapMainEntry) + sizeof(uint32_t) + record.varSizeFieldCount() * sizeof(ColumnMapHeapEntry);
}

/**
 * @brief Calculate the number of elements fitting in one page excluding any variable sized fields
 */
uint32_t calcFixedSizeCapacity(const Record& record) {
    return ColumnMapContext::MAX_DATA_SIZE / (calcEntryOverhead(record) + record.fixedSize());
}

} // anonymous namespace

ColumnMapContext::ColumnMapContext(const PageManager& pageManager, const Record& record)
        : mPageData(reinterpret_cast<uintptr_t>(pageManager.data())),
          mEntryOverhead(calcEntryOverhead(record)),
          mFixedSizeCapacity(calcFixedSizeCapacity(record)),
          mFixedSize(record.fixedSize()),
          mVariableSizeOffset(record.variableSizeOffset()),
          mVarSizeFieldCount(record.varSizeFieldCount()) {
    mFieldLengths.reserve(record.fixedSizeFieldCount() + 1);

    uint32_t startOffset = record.headerSize();
    if (startOffset > 0) {
        mFieldLengths.emplace_back(startOffset);
    }
    LOG_ASSERT(record.fixedSizeFieldCount() == 0 || record.getFieldMeta(0).second == static_cast<int32_t>(startOffset),
            "First field must point to end of header");

    for (decltype(record.fixedSizeFieldCount()) i = 1; i < record.fixedSizeFieldCount(); ++i) {
        auto endOffset = record.getFieldMeta(i).second;
        LOG_ASSERT(endOffset >= 0, "Offset must be positive");
        LOG_ASSERT(endOffset > static_cast<int32_t>(startOffset), "Offset must be larger than start offset");
        mFieldLengths.emplace_back(static_cast<uint32_t>(endOffset) - startOffset);
        startOffset = static_cast<uint32_t>(endOffset);
    }

    if (record.fixedSizeFieldCount() != 0u) {
        mFieldLengths.emplace_back(static_cast<uint32_t>(record.fixedSize()) - startOffset);
    }

    // Build and compile Materialize function via LLVM
    prepareMaterializeFunction();
}

void ColumnMapContext::prepareMaterializeFunction() {
    using namespace llvm;

#ifndef NDEBUG
    LOG_INFO("Generating LLVM materialize function");
    auto startTime = std::chrono::steady_clock::now();
#endif

    // Create LLVM context and module
    LLVMContext context;
    Module module("Materialize", context);
    module.setDataLayout(mLLVMJit.getTargetMachine()->createDataLayout());
    module.setTargetTriple(mLLVMJit.getTargetMachine()->getTargetTriple().getTriple());

    LLVMBuilder builder(context);

    // Create function
    std::array<Type*, 6> params = {{
        builder.getInt8PtrTy(), // recordData
        builder.getInt8PtrTy(), // heapData
        builder.getInt64Ty(),   // count
        builder.getInt64Ty(),   // idx
        builder.getInt8PtrTy(), // dest
        builder.getInt64Ty()    // size
    }};
    auto funcType = FunctionType::get(builder.getVoidTy(), makeArrayRef(&params.front(), params.size()), false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gMaterializeFunctionName, &module);

    // Set arguments names
    std::array<Value*, 6> args;
    {
        decltype(gMaterializeParamNames.size()) idx = 0;
        for (auto iter = func->arg_begin(); idx != gMaterializeParamNames.size(); ++iter, ++idx) {
            iter->setName(gMaterializeParamNames[idx]);
            args[idx] = iter.operator ->();
        }
    }

    // Set noalias hints (data pointers are not allowed to overlap)
    func->setDoesNotAlias(1);
    func->setDoesNotAlias(2);
    func->setDoesNotAlias(5);

    // Add host CPU features
    func->addFnAttr(Attribute::NoUnwind);
    func->addFnAttr("target-cpu", mLLVMJit.getTargetMachine()->getTargetCPU());
    func->addFnAttr("target-features", mLLVMJit.getTargetMachine()->getTargetFeatureString());

    // Build function
    auto bb = BasicBlock::Create(context, "entry", func);
    builder.SetInsertPoint(bb);

    // Build function body
    auto recordData = args[0];
    auto dest = args[4];

    // Copy all fixed size fields including the header (null bitmap) if the record has one
    typename decltype(mFieldLengths)::value_type lastFieldLength = 0;
    for (auto fieldLength : mFieldLengths) {
        if (lastFieldLength != 0u) {
            // -> dest += fieldLength
            dest = builder.CreateInBoundsGEP(dest, builder.getInt64(lastFieldLength));

            // -> recordData += page->count * fieldLength;
            recordData = builder.CreateInBoundsGEP(recordData, builder.createConstMul(args[2], lastFieldLength));
        }

        // -> auto src = recordData + idx * fieldLength;
        auto src = builder.CreateInBoundsGEP(recordData, builder.createConstMul(args[3], fieldLength));

        // -> memcpy(dest, src, fieldLength);
        builder.CreateMemCpy(dest, src, builder.getInt64(fieldLength), fieldLength > 8u ? 8u : fieldLength);

        lastFieldLength = fieldLength;
    }

    // Copy all variable size fields in one batch
    if (mVarSizeFieldCount != 0u) {
        if (lastFieldLength != 0u) {
            // -> dest = crossbow::align(dest + fieldLength, 4u);
            dest = builder.CreateInBoundsGEP(args[4], builder.getInt64(mVariableSizeOffset));

            // -> recordData += page->count * fieldLength;
            recordData = builder.CreateInBoundsGEP(recordData, builder.createConstMul(args[2], lastFieldLength));

            if (lastFieldLength < 8u) {
                // -> recordData = crossbow::align(recordData, 8u);
                recordData = builder.createPointerAlign(recordData, 8u);
            }
        }

        // -> auto offset = reinterpret_cast<const ColumnMapHeapEntry*>(recordData)[idx].offset;
        static_assert(offsetof(ColumnMapHeapEntry, offset) == 0, "Offset of ColumnMapHeapEntry::offset must be 0");
        auto heapOffset = builder.CreateInBoundsGEP(recordData,
                builder.createConstMul(args[3], sizeof(ColumnMapHeapEntry)));
        heapOffset = builder.CreateAlignedLoad(builder.CreateBitCast(heapOffset, builder.getInt32PtrTy()), 8u);

        // -> auto src = page->heapData() - offset;
        auto src = builder.CreateGEP(args[1], builder.CreateNeg(builder.CreateZExt(heapOffset, builder.getInt64Ty())));

        // -> auto length = size - crossbow::align(mFixedSize, 4u);
        auto length = builder.CreateSub(args[5], builder.getInt64(crossbow::align(mFixedSize, 4u)));

        // -> memcpy(dest, src, length);
        builder.CreateMemCpy(dest, src, length, 4u);
    }

    // Return
    builder.CreateRetVoid();

#ifndef NDEBUG
    LOG_INFO("Dumping LLVM Code before optimizations");
    module.dump();
#endif

    // Setup optimizations
    legacy::PassManager modulePass;
#ifndef NDEBUG
    modulePass.add(createVerifierPass());
#endif
    modulePass.add(createTargetTransformInfoWrapperPass(mLLVMJit.getTargetMachine()->getTargetIRAnalysis()));

    legacy::FunctionPassManager functionPass(&module);
#ifndef NDEBUG
    functionPass.add(createVerifierPass());
#endif
    functionPass.add(createTargetTransformInfoWrapperPass(mLLVMJit.getTargetMachine()->getTargetIRAnalysis()));

    PassManagerBuilder optimizationBuilder;
    optimizationBuilder.OptLevel = 2;
    optimizationBuilder.LoadCombine = true;
    optimizationBuilder.populateFunctionPassManager(functionPass);
    optimizationBuilder.populateModulePassManager(modulePass);
    optimizationBuilder.populateLTOPassManager(modulePass);

    functionPass.doInitialization();
    functionPass.run(*func);
    functionPass.doFinalization();

    modulePass.run(module);

#ifndef NDEBUG
    LOG_INFO("Dumping LLVM Code after optimizations");
    module.dump();
#endif

    // Compile the module
    mLLVMJit.addModule(&module);

    // Get function pointer for materialization function
    mMaterializeFun = mLLVMJit.findFunction<MaterializeFun>(gMaterializeFunctionName);

#ifndef NDEBUG
    auto endTime = std::chrono::steady_clock::now();
    LOG_INFO("Generating LLVM materialize function took %1%ns", (endTime - startTime).count());
#endif
}

} // namespace deltamain
} // namespace store
} // namespace tell
