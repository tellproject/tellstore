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
    "destData",
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
    prepareMaterializeFunction(record);
}

void ColumnMapContext::prepareMaterializeFunction(const Record& record) {
    using namespace llvm;

    static constexpr size_t recordData = 0;
    static constexpr size_t heapData = 1;
    static constexpr size_t count = 2;
    static constexpr size_t idx = 3;
    static constexpr size_t destData = 4;
    static constexpr size_t size = 5;

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
    auto funcType = FunctionType::get(builder.getVoidTy(), {
            builder.getInt8PtrTy(), // recordData
            builder.getInt8PtrTy(), // heapData
            builder.getInt64Ty(),   // count
            builder.getInt64Ty(),   // idx
            builder.getInt8PtrTy(), // destData
            builder.getInt64Ty()    // size
    }, false);
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
    func->setOnlyReadsMemory(1);
    func->setDoesNotAlias(2);
    func->setOnlyReadsMemory(2);
    func->setDoesNotAlias(5);

    // Add host CPU features
    func->addFnAttr(Attribute::NoUnwind);
    func->addFnAttr("target-cpu", mLLVMJit.getTargetMachine()->getTargetCPU());
    func->addFnAttr("target-features", mLLVMJit.getTargetMachine()->getTargetFeatureString());

    // Create ColumnMapHeapEntries struct
    static_assert(sizeof(ColumnMapHeapEntry) == 8, "Size of ColumnMapHeapEntry must be 8");
    static_assert(offsetof(ColumnMapHeapEntry, offset) == 0, "Offset of ColumnMapHeapEntry::offset must be 0");
    static_assert(offsetof(ColumnMapHeapEntry, prefix) == 4, "Offset of ColumnMapHeapEntry::prefix must be 4");
    auto heapEntriesStructType = StructType::get(context, {
            builder.getInt32Ty(),   // offset
            builder.getInt32Ty()    // prefix
    });
    heapEntriesStructType->setName("HeapEntries");

    // Build function
    auto bb = BasicBlock::Create(context, "entry", func);
    builder.SetInsertPoint(bb);

    // Copy the header (null bitmap) if the record has one
    if (record.headerSize() != 0) {
        // -> auto src = page->recordData() + idx * record.headerSize();
        auto src = builder.CreateInBoundsGEP(args[recordData], builder.createConstMul(args[idx], record.headerSize()));

        // -> memcpy(data, src, record.headerSize());
        builder.CreateMemCpy(args[destData], src, record.headerSize(), 8u);
    }

    // Copy all fixed size fields
    for (size_t i = 0; i < record.fixedSizeFieldCount(); ++i) {
        auto& fieldMeta = record.getFieldMeta(i);
        auto& field = fieldMeta.first;
        auto fieldOffset = fieldMeta.second;
        auto fieldAlignment = field.alignOf();
        auto fieldPtrTy = builder.getFieldPtrTy(field.type());

        // -> auto src = reinterpret_cast<const T*>(page->recordData() + page->count * fieldOffset) + idx;
        auto src = args[recordData];
        if (fieldOffset != 0u) {
            src = builder.CreateInBoundsGEP(src, builder.createConstMul(args[count], fieldOffset));
        }
        src = builder.CreateInBoundsGEP(builder.CreateBitCast(src, fieldPtrTy), args[idx]);

        // -> auto dest = reinterpret_cast<const T*>(data + fieldOffset);
        auto dest = args[destData];
        if (fieldOffset != 0u) {
            dest = builder.CreateInBoundsGEP(dest, builder.getInt64(fieldOffset));
        }
        dest = builder.CreateBitCast(dest, fieldPtrTy);

        // -> *dest = *src;
        builder.CreateAlignedStore(builder.CreateAlignedLoad(src, fieldAlignment), dest, fieldAlignment);
    }

    // Copy all variable size fields in one batch
    if (mVarSizeFieldCount != 0u) {
        // -> auto heapEntries = crossbow::align(page->recordData() + page->count * mFixedSize, 8u);
        auto heapEntries = args[0];
        if (mFixedSize != 0u) {
            heapEntries = builder.createPointerAlign(
                    builder.CreateInBoundsGEP(heapEntries, builder.createConstMul(args[count], mFixedSize)),
                    8u);
        }

        // -> auto offset = *reinterpret_cast<const ColumnMapHeapEntry*>(heapEntries)[idx].offset;
        auto offset = builder.CreateInBoundsGEP(
                builder.CreateBitCast(heapEntries, heapEntriesStructType->getPointerTo()),
                { args[idx], builder.getInt32(0) });
        offset = builder.CreateAlignedLoad(offset, 8u);

        // -> auto src = page->heapData() - offset;
        auto src = builder.CreateGEP(
                args[heapData],
                builder.CreateNeg(builder.CreateZExt(offset, builder.getInt64Ty())));

        // -> auto dest = data + mVariableSizeOffset;
        auto dest = args[destData];
        if (mVariableSizeOffset != 0u) {
            dest = builder.CreateInBoundsGEP(dest, builder.getInt64(mVariableSizeOffset));
        }

        // -> auto length = size - mVariableSizeOffset;
        auto length = args[size];
        if (mVariableSizeOffset != 0u) {
            length = builder.CreateSub(length, builder.getInt64(mVariableSizeOffset));
        }

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
