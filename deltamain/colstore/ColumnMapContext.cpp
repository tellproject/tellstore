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

#include "LLVMColumnMapMaterialize.hpp"

#include <tellstore/Record.hpp>
#include <util/PageManager.hpp>

#include <crossbow/logger.hpp>

#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>

namespace tell {
namespace store {
namespace deltamain {

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
#ifndef NDEBUG
    LOG_INFO("Generating LLVM materialize function");
    auto startTime = std::chrono::steady_clock::now();
#endif

    // Create LLVM context and module
    llvm::LLVMContext context;
    llvm::Module module("Materialize", context);
    module.setDataLayout(mLLVMJit.getTargetMachine()->createDataLayout());
    module.setTargetTriple(mLLVMJit.getTargetMachine()->getTargetTriple().getTriple());

    LLVMColumnMapMaterializeBuilder::createFunction(*this, module, mLLVMJit.getTargetMachine());

#ifndef NDEBUG
    LOG_INFO("Dumping LLVM Code before optimizations");
    module.dump();
#endif

    // Setup optimizations
    llvm::legacy::PassManager modulePass;
#ifndef NDEBUG
    modulePass.add(llvm::createVerifierPass());
#endif
    modulePass.add(llvm::createTargetTransformInfoWrapperPass(mLLVMJit.getTargetMachine()->getTargetIRAnalysis()));

    llvm::legacy::FunctionPassManager functionPass(&module);
#ifndef NDEBUG
    functionPass.add(llvm::createVerifierPass());
#endif
    functionPass.add(llvm::createTargetTransformInfoWrapperPass(mLLVMJit.getTargetMachine()->getTargetIRAnalysis()));

    llvm::PassManagerBuilder optimizationBuilder;
    optimizationBuilder.OptLevel = 2;
    optimizationBuilder.LoadCombine = true;
    optimizationBuilder.populateFunctionPassManager(functionPass);
    optimizationBuilder.populateModulePassManager(modulePass);
    optimizationBuilder.populateLTOPassManager(modulePass);

    functionPass.doInitialization();
    for (auto& fun : module) {
        functionPass.run(fun);
    }
    functionPass.doFinalization();

    modulePass.run(module);

#ifndef NDEBUG
    LOG_INFO("Dumping LLVM Code after optimizations");
    module.dump();
#endif

    // Compile the module
    mLLVMJit.addModule(&module);

    // Get function pointer for materialization function
    mMaterializeFun = mLLVMJit.findFunction<LLVMColumnMapMaterializeBuilder::Signature>(
            LLVMColumnMapMaterializeBuilder::FUNCTION_NAME);

#ifndef NDEBUG
    auto endTime = std::chrono::steady_clock::now();
    LOG_INFO("Generating LLVM materialize function took %1%ns", (endTime - startTime).count());
#endif
}

} // namespace deltamain
} // namespace store
} // namespace tell
