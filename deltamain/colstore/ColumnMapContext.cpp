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

#include "LLVMColumnMapUtils.hpp"

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

static const std::array<std::string, 4> gMaterializeParamNames = {{
    "page",
    "idx",
    "destData",
    "size"
}};

} // anonymous namespace

ColumnMapContext::ColumnMapContext(const PageManager& pageManager, const Record& record)
        : mRecord(record),
          mPageData(reinterpret_cast<uintptr_t>(pageManager.data())),
          mHeaderSize(mRecord.headerSize()),
          mFixedSize(0u) {
    mFixedMetaData.reserve(mRecord.fixedSizeFieldCount());
    for (decltype(mRecord.fixedSizeFieldCount()) i = 0; i < mRecord.fixedSizeFieldCount(); ++i) {
        auto& field = mRecord.getFieldMeta(i).field;
        auto fieldLength = field.staticSize();

        mFixedMetaData.emplace_back(mFixedSize, fieldLength);
        mFixedSize += fieldLength;
    }

    mStaticSize = sizeof(ColumnMapMainEntry) + sizeof(uint32_t) + mHeaderSize + mFixedSize
            + mRecord.varSizeFieldCount() * sizeof(ColumnMapHeapEntry);
    mStaticCapacity = MAX_DATA_SIZE / mStaticSize;

    // Build and compile Materialize function via LLVM
    prepareMaterializeFunction();
}

void ColumnMapContext::prepareMaterializeFunction() {
    using namespace llvm;

    static constexpr size_t page = 0;
    static constexpr size_t idx = 1;
    static constexpr size_t destData = 2;
    static constexpr size_t size = 3;

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

    // Create ColumnMapMainPage struct
    auto mainPageStructType = getColumnMapMainPageTy(context);

    // Create ColumnMapHeapEntries struct
    auto heapEntriesStructType = getColumnMapHeapEntriesTy(context);

    // Create function
    auto funcType = FunctionType::get(builder.getVoidTy(), {
            builder.getInt8PtrTy(), // page
            builder.getInt64Ty(),   // idx
            builder.getInt8PtrTy(), // destData
            builder.getInt64Ty()    // size
    }, false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gMaterializeFunctionName, &module);

    // Set arguments names
    std::array<Value*, 4> args;
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
    func->setDoesNotAlias(3);

    // Add host CPU features
    func->addFnAttr(Attribute::NoUnwind);
    func->addFnAttr("target-cpu", mLLVMJit.getTargetMachine()->getTargetCPU());
    func->addFnAttr("target-features", mLLVMJit.getTargetMachine()->getTargetFeatureString());

    // Build function
    auto entryBlock = BasicBlock::Create(context, "entry", func);
    builder.SetInsertPoint(entryBlock);

    // Copy the header (null bitmap) if the record has one
    auto mainPage = builder.CreateBitCast(args[page], mainPageStructType->getPointerTo());
    if (mRecord.headerSize() != 0) {
        // auto headerOffset = mainPage->headerOffset;
        auto headerOffset = builder.CreateInBoundsGEP(mainPage, { builder.getInt64(0), builder.getInt32(1) });
        headerOffset = builder.CreateZExt(builder.CreateAlignedLoad(headerOffset, 4u), builder.getInt64Ty());

        // -> auto src = page + headerOffset;
        auto src = builder.CreateInBoundsGEP(args[page], headerOffset);

        // -> src += idx * record.headerSize();
        src = builder.CreateInBoundsGEP(src, builder.createConstMul(args[idx], mRecord.headerSize()));

        // -> memcpy(data, src, record.headerSize());
        builder.CreateMemCpy(args[destData], src, mRecord.headerSize(), 8u);
    }

    auto count = builder.CreateInBoundsGEP(mainPage, { builder.getInt64(0), builder.getInt32(0) });
    count = builder.CreateZExt(builder.CreateAlignedLoad(count, 4u), builder.getInt64Ty());

    // Copy all fixed size fields
    if (mRecord.fixedSizeFieldCount() != 0) {
        // auto fixedOffset = mainPage->fixedOffset;
        auto fixedOffset = builder.CreateInBoundsGEP(mainPage, { builder.getInt64(0), builder.getInt32(2) });
        fixedOffset = builder.CreateZExt(builder.CreateAlignedLoad(fixedOffset, 4u), builder.getInt64Ty());

        // -> auto fixedData = page + fixedOffset;
        auto fixedData = builder.CreateInBoundsGEP(args[page], fixedOffset);

        for (decltype(mRecord.fixedSizeFieldCount()) i = 0; i < mRecord.fixedSizeFieldCount(); ++i) {
            auto& rowMeta = mRecord.getFieldMeta(i);
            auto& field = rowMeta.field;
            auto fieldAlignment = field.alignOf();
            auto fieldPtrTy = builder.getFieldPtrTy(field.type());

            // -> auto src = reinterpret_cast<const T*>(fixedData + page->count * fieldOffset) + idx;
            auto src = fixedData;
            if (mFixedMetaData[i].offset != 0) {
                src = builder.CreateInBoundsGEP(src, builder.createConstMul(count, mFixedMetaData[i].offset));
            }
            src = builder.CreateBitCast(src, fieldPtrTy);
            src = builder.CreateInBoundsGEP(src, args[idx]);

            // -> auto value = *src;
            auto value = builder.CreateAlignedLoad(src, fieldAlignment);

            // -> auto dest = reinterpret_cast<const T*>(data + fieldOffset);
            auto dest = args[destData];
            if (rowMeta.offset != 0u) {
                dest = builder.CreateInBoundsGEP(dest, builder.getInt64(rowMeta.offset));
            }
            dest = builder.CreateBitCast(dest, fieldPtrTy);

            // -> *dest = value;
            builder.CreateAlignedStore(value, dest, fieldAlignment);
        }
    }

    // Copy all variable size fields in one batch
    if (mRecord.varSizeFieldCount() != 0u) {
        // auto variableOffset = mainPage->variableOffset;
        auto variableOffset = builder.CreateInBoundsGEP(mainPage, { builder.getInt64(0), builder.getInt32(3) });
        variableOffset = builder.CreateZExt(builder.CreateAlignedLoad(variableOffset, 4u), builder.getInt64Ty());

        // -> auto src = reinterpret_cast<const ColumnMapHeapEntry*>(page + variableOffset) + idx;
        auto src = builder.CreateInBoundsGEP(args[page], variableOffset);
        src = builder.CreateBitCast(src, heapEntriesStructType->getPointerTo());
        src = builder.CreateInBoundsGEP(src, args[idx]);

        // -> auto startOffset = src->offset;
        auto startOffset = builder.CreateInBoundsGEP(src, { builder.getInt64(0), builder.getInt32(0) });
        startOffset = builder.CreateAlignedLoad(startOffset, 8u);

        // The first offset is always the static size of the row record
        {
            auto& rowMeta = mRecord.getFieldMeta(mRecord.fixedSizeFieldCount());

            // -> auto dest = reinterpret_cast<uint32_t*>(data + fieldOffset);
            auto dest = args[destData];
            if (rowMeta.offset != 0) {
                dest = builder.CreateInBoundsGEP(dest, builder.getInt64(rowMeta.offset));
            }
            dest = builder.CreateBitCast(dest, builder.getInt32PtrTy());

            // -> *dest = record.staticSize();
            builder.CreateAlignedStore(builder.getInt32(mRecord.staticSize()), dest, 4u);
        }

        // Offsets of the remaining fields have to be calculated
        if (mRecord.varSizeFieldCount() > 1) {
            // -> auto offsetCorrection = startOffset + record.staticSize();
            auto offsetCorrection = builder.CreateAdd(startOffset, builder.getInt32(mRecord.staticSize()));
            for (decltype(mRecord.varSizeFieldCount()) i = 1; i < mRecord.varSizeFieldCount(); ++i) {
                // -> src += count;
                src = builder.CreateInBoundsGEP(src, count);

                // -> auto offset = src->offset - offsetCorrection;
                auto offset = builder.CreateInBoundsGEP(src, { builder.getInt64(0), builder.getInt32(0) });
                offset = builder.CreateAlignedLoad(offset, 8u);
                offset = builder.CreateSub(offsetCorrection, offset);

                auto& rowMeta = mRecord.getFieldMeta(mRecord.fixedSizeFieldCount() + i);

                // -> auto dest = reinterpret_cast<uint32_t*>(data + fieldOffset);
                auto dest = builder.CreateBitCast(
                        builder.CreateInBoundsGEP(args[destData], builder.getInt64(rowMeta.offset)),
                        builder.getInt32PtrTy());

                // -> *dest = offset;
                builder.CreateAlignedStore(offset, dest, 4u);
            }
        }

        // The last offset is always the size
        {
            // -> auto offset = static_cast<uint32_t>(size);
            auto endOffset = builder.CreateTrunc(args[size], builder.getInt32Ty());

            // -> auto dest = reinterpret_cast<uint32_t*>(dest + record.staticSize() - sizeof(uint32_t));
            auto dest = builder.CreateInBoundsGEP(
                    args[destData],
                    builder.getInt64(mRecord.staticSize() - sizeof(uint32_t)));
            dest = builder.CreateBitCast(dest, builder.getInt32PtrTy());

            // -> *dest = offset;
            builder.CreateAlignedStore(endOffset, dest, 4u);
        }

        // -> auto heapOffset = TELL_PAGE_SIZE - static_cast<uint64_t>(startOffset);
        auto heapOffset = builder.CreateZExt(startOffset, builder.getInt64Ty());
        heapOffset = builder.CreateSub(builder.getInt64(TELL_PAGE_SIZE), heapOffset);

        // -> auto heapSrc = page + heapOffset;
        auto heapSrc = builder.CreateInBoundsGEP(args[page], heapOffset);

        // -> auto dest = data + record.staticSize();
        auto dest = builder.CreateInBoundsGEP(args[destData], builder.getInt64(mRecord.staticSize()));

        // -> auto length = size - record.staticSize();
        auto length = builder.CreateSub(args[size], builder.getInt64(mRecord.staticSize()));

        // -> memcpy(dest, heapSrc, length);
        builder.CreateMemCpy(dest, heapSrc, length, 1u);
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
