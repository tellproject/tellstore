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

#include <util/PageManager.hpp>

#include <crossbow/logger.hpp>

#include <llvm/ADT/APInt.h>
#include <llvm/ADT/ArrayRef.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Verifier.h>
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
 * @brief Returns the exact log2 if it exists and -1 otherwise
 */
int32_t computeLog2(uint32_t val) {
    LOG_ASSERT(val!=0, "");
    if (val & (val - 1))
        return -1;

    int32_t result = 0;
    while (true) {
        if (val & 0x1)
            return result;
        val >>= 1;
        ++result;
    }
    return -1;
}

llvm::Value* creatMulOrShift(llvm::LLVMContext& context, llvm::IRBuilder<>& builder, llvm::Value* lhs, uint factor) {
    using namespace llvm;

    int log2 = computeLog2(factor);
    if (factor > 0)
        return builder.CreateShl(lhs, ConstantInt::get(context, APInt(32, log2)));
    else
        return builder.CreateMul(lhs, ConstantInt::get(context, APInt(32, factor)));
}

llvm::Value* createPointerAlign(llvm::LLVMContext& context, llvm::IRBuilder<>& builder, llvm::Value* value,
        uintptr_t alignment) {
    using namespace llvm;

    // -> auto result = reinterpret_cast<uintptr_t>(value);
    auto result = builder.CreatePtrToInt(value, Type::getIntNTy(context, sizeof(uintptr_t) * 8));
    // -> result = result - 1u + alignment;
    result = builder.CreateAdd(result, ConstantInt::get(context, APInt(sizeof(uintptr_t) * 8, alignment - 1u)));
    // -> result = result & -alignment;
    result = builder.CreateAnd(result, ConstantInt::get(context, APInt(sizeof(uintptr_t) * 8, -alignment)));
    // -> recordData = reinterpret_cast<const char*>(recordDataPtr);
    result = builder.CreateIntToPtr(result, Type::getInt8PtrTy(context));

    return result;
}

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
          mVarSizeFieldCount(record.varSizeFieldCount()) {
    mFieldLengths.reserve(record.fixedSizeFieldCount() + 1);

    uint32_t startOffset = record.headerSize();
    if (startOffset > 0) {
        mFieldLengths.emplace_back(startOffset);
    }
    LOG_ASSERT(record.getFieldMeta(0).second == static_cast<int32_t>(startOffset),
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

    // Build LLVM
    mMaterialize = generateMaterializeFunc();
}

ColumnMapContext::MaterializeFunc ColumnMapContext::generateMaterializeFunc() {
    using namespace llvm;

#ifndef NDEBUG
    LOG_TRACE("Generating LLVM materialize function");
    auto startTime = std::chrono::steady_clock::now();
#endif

    // Create LLVM context and module
    LLVMContext context;
    Module module("Materialize", context);
    module.setDataLayout(mLLVMJit.getTargetMachine()->createDataLayout());
    module.setTargetTriple(mLLVMJit.getTargetMachine()->getTargetTriple().getTriple());

    // Create function
    std::array<Type*, 6> params = {{
        Type::getInt8PtrTy(context),    // recordData
        Type::getInt8PtrTy(context),    // heapData
        Type::getInt32Ty(context),      // count
        Type::getInt64Ty(context),      // idx
        Type::getInt8PtrTy(context),    // dest
        Type::getInt64Ty(context)       // size
    }};
    auto funcType = FunctionType::get(Type::getVoidTy(context), makeArrayRef(&params.front(), params.size()), false);
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

    // Add host CPU features
    func->addFnAttr(Attribute::NoUnwind);
    func->addFnAttr("target-cpu", mLLVMJit.getTargetMachine()->getTargetCPU());
    func->addFnAttr("target-features", mLLVMJit.getTargetMachine()->getTargetFeatureString());

    // Get memcpy intrinsic function
    std::array<Type*, 5> memcpyCallParams = {{
        Type::getInt8PtrTy(context),    // dest
        Type::getInt8PtrTy(context),    // src
        Type::getInt64Ty(context),      // len
        Type::getInt32Ty(context),      // align
        Type::getInt1Ty(context)        // isvolatile
    }};
    auto memcpyFunc = Intrinsic::getDeclaration(&module, Intrinsic::memcpy, makeArrayRef(&memcpyCallParams.front(),
            memcpyCallParams.size()));

    // Build function
    IRBuilder<> builder(context);
    auto bb = BasicBlock::Create(context, "entry", func);
    builder.SetInsertPoint(bb);

    // Set alignment hints (all pointers are 8 byte aligned)
    builder.CreateAlignmentAssumption(module.getDataLayout(), args[0], 8u);
    builder.CreateAlignmentAssumption(module.getDataLayout(), args[1], 8u);
    builder.CreateAlignmentAssumption(module.getDataLayout(), args[4], 8u);

    // Build function body
    auto recordData = args[0];
    auto dest = args[4];

    // Copy all fixed size fields including the header (null bitmap) if the record has one
    typename decltype(mFieldLengths)::value_type lastFieldLength = 0;
    for (auto fieldLength : mFieldLengths) {
        if (lastFieldLength != 0u) {
            // -> dest += fieldLength
            dest = builder.CreateAdd(dest, ConstantInt::get(context, APInt(64, lastFieldLength)));

            // -> recordData += page->count * fieldLength;
            recordData = builder.CreateGEP(recordData, creatMulOrShift(context, builder, args[2], lastFieldLength));
        }

        // -> auto src = recordData + idx * fieldLength;
        auto src = builder.CreateGEP(recordData, creatMulOrShift(context, builder, args[3], fieldLength));

        // -> memcpy(dest, src, fieldLength);
        std::array<Value*, 5> memcpyValues = {{
            dest,
            src,
            ConstantInt::get(context, APInt(64, fieldLength)),
            ConstantInt::get(context, APInt(32, (fieldLength > 8u ? 8u : fieldLength))),
            ConstantInt::getFalse(context)
        }};
        builder.CreateCall(memcpyFunc, makeArrayRef(&memcpyValues.front(), memcpyValues.size()));

        lastFieldLength = fieldLength;
    }

    // Copy all variable size fields in one batch
    if (mVarSizeFieldCount != 0u) {
        if (lastFieldLength != 0u) {
            // -> dest += crossbow::align(fieldLength, 4u);
            auto alignedFieldLength = crossbow::align(lastFieldLength, 4u);
            dest = builder.CreateAdd(dest, ConstantInt::get(context, APInt(64, alignedFieldLength)));
            builder.CreateAlignmentAssumption(module.getDataLayout(), dest, 4u);

            // -> recordData += page->count * fieldLength;
            recordData = builder.CreateGEP(recordData, creatMulOrShift(context, builder, args[2], lastFieldLength));

            if (lastFieldLength < 8u) {
                // -> recordData = crossbow::align(recordData, 8u);
                recordData = createPointerAlign(context, builder, recordData, 8u);
            }
            builder.CreateAlignmentAssumption(module.getDataLayout(), recordData, 8u);
        }

        // -> auto offset = reinterpret_cast<const ColumnMapHeapEntry*>(recordData)[idx].offset;
        static_assert(offsetof(ColumnMapHeapEntry, offset) == 0, "Offset of ColumnMapHeapEntry::offset must be 0");
        auto heapOffset = builder.CreateGEP(recordData, creatMulOrShift(context, builder, args[3],
                sizeof(ColumnMapHeapEntry)));
        heapOffset = builder.CreateLoad(builder.CreateBitCast(heapOffset, Type::getInt32PtrTy(context)));

        // -> auto src = page->heapData() - offset;
        auto heapOffsetSub = builder.CreateSub(ConstantInt::get(context, APInt(32, 0)), heapOffset);
        auto src = builder.CreateGEP(args[1], heapOffsetSub);

        // -> auto length = size - crossbow::align(mFixedSize, 4u);
        auto length = builder.CreateSub(args[5], ConstantInt::get(context, APInt(64, crossbow::align(mFixedSize, 4u))));

        // -> memcpy(dest, src, length);
        std::array<Value*, 5> memcpyValues = {{
            dest,
            src,
            length,
            ConstantInt::get(context, APInt(32, 4u)),
            ConstantInt::getFalse(context)
        }};
        builder.CreateCall(memcpyFunc, makeArrayRef(&memcpyValues.front(), memcpyValues.size()));
    }

    // Return
    builder.CreateRetVoid();

    LOG_ASSERT(verifyFunction(*func), "LLVM Code Generation for ColumnMap materialize failed!");

#ifndef NDEBUG
    LOG_TRACE("Dumping LLVM Code before optimizations");
    module.dump();
#endif

    // Setup optimizations
    legacy::FunctionPassManager optimizer(&module);
    // Provide basic AliasAnalysis support for GVN
    optimizer.add(createBasicAliasAnalysisPass());
    // Do simple "peephole" optimizations and bit-twiddling optimizations
    optimizer.add(createInstructionCombiningPass());
    // Eliminate Common SubExpressions
    optimizer.add(createGVNPass());
    // Simplify the control flow graph (deleting unreachable blocks, etc)
    optimizer.add(createCFGSimplificationPass());

    // Run optimizations
    optimizer.doInitialization();
    optimizer.run(*func);

#ifndef NDEBUG
    LOG_TRACE("Dumping LLVM Code after optimizations");
    module.dump();
#endif

    // Compile the module
    mLLVMJit.addModule(&module);

    // Get function pointer for materialization function
    auto materializeSymbol = mLLVMJit.findSymbol(gMaterializeFunctionName);
    LOG_ASSERT(materializeSymbol, "Couldn't find function symbol in jit module");
    auto res = reinterpret_cast<MaterializeFunc>(materializeSymbol.getAddress());

#ifndef NDEBUG
    auto endTime = std::chrono::steady_clock::now();
    LOG_TRACE("Generating LLVM materialize function took %1%ns", (endTime - startTime).count());
#endif

    return res;
}

} // namespace deltamain
} // namespace store
} // namespace tell
