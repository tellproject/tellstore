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

#include "LLVMCodeGenerator.hpp"
#include "llvm/ADT/STLExtras.h"
#include "llvm/Analysis/Passes.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Vectorize.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/Bitcode/BitcodeWriterPass.h"
#include "llvm/IR/Intrinsics.h"

#include <cctype>
#include <string>
#include <vector>

#include <deltamain/colstore/ColumnMapContext.hpp>

using namespace llvm;
using namespace llvm::orc;
using namespace tell::store::deltamain;

namespace tell {
namespace store {

/**
 * @brief The LLVMCodeGenerator class is used as a utility class to all LLVM-related
 * operations in TELL.
 *
 * The typical usage is to first create an LLVMContext (use the getGlobalContext if
 * you are single-threaded) and an LLVMJIT (using getJIT). Next, you can create as
 * many LLVM modules as you like using the created context and jit (using the get
 * methods of this classe).
 * For code creation
 * in a module you might want to use an IRBuilder which you create with the same
 * context as the module.
 *
 * Code in a module can be executed in the following way using its jit:
 * - auto H = jit->addModule(std::move(module));
 * - auto exprSymbol = jit->findSymbol(...);
 * - ret-type (*funcPtr) (args) = (ret-type (*)(args))(intptr_t) exprSymbol.getAddress();
 * - use funcPtr to execute the function(s) as often as you like
 * - jit->removeModule(H);
 *
 * You can optimize certain functions in a module by using a FunctionPassManager which
 * you can get with the constructor listed in this class. Then simply call
 * run(function) on the function you want to optimize.
 *
 * If you use the functions
 */

std::unique_ptr<LLVMJIT> LLVMCodeGenerator::getJIT() {
    if (!mInitialized)
        initialize();
    return std::move(llvm::make_unique<LLVMJIT>());
}

std::unique_ptr<Module> LLVMCodeGenerator::getModule(LLVMJIT *jit, LLVMContext &context, std::string name) {
    std::unique_ptr<Module> module = llvm::make_unique<Module>(name, context);
    module->setDataLayout(jit->getTargetMachine().createDataLayout());
    return std::move(module);
}

std::unique_ptr<legacy::FunctionPassManager> LLVMCodeGenerator::getFunctionPassManger(Module *module) {
    // create a function pass manager attached to the module with some optimizations
    std::unique_ptr<legacy::FunctionPassManager> functionPassMgr = llvm::make_unique<legacy::FunctionPassManager>(module);
    // Provide basic AliasAnalysis support for GVN.
    functionPassMgr->add(createBasicAliasAnalysisPass());
    // Do simple "peephole" optimizations and bit-twiddling optzns.
    functionPassMgr->add(createInstructionCombiningPass());
    // Reassociate expressions.
    functionPassMgr->add(createReassociatePass());
    // Eliminate Common SubExpressions.
    functionPassMgr->add(createGVNPass());
    // Simplify the control flow graph (deleting unreachable blocks, etc).
    functionPassMgr->add(createCFGSimplificationPass());
    // add basic block vectorization
    functionPassMgr->add(createBBVectorizePass());
    // add loop vectorization
    functionPassMgr->add(createLoopVectorizePass());
    // initialize function pass manager
    functionPassMgr->doInitialization();
    return std::move(functionPassMgr);
}

// returns the exact log2 if it exists and -1 otherwise
inline int32_t computeLog2(uint32_t val)
{
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

inline Value* creatMulOrShift(LLVMContext& context, IRBuilder<> &builder, Value *LHS, uint factor)
{
    int log2 = computeLog2(factor);
    if (factor > 0)
        return builder.CreateShl(LHS,ConstantInt::get(context, APInt(32, log2)));
    else
        return builder.CreateMul(LHS,ConstantInt::get(context, APInt(32, factor)));
}

LLVMCodeGenerator::MaterializeFuncPtr LLVMCodeGenerator::generate_colmap_materialize_function(
        llvm::orc::LLVMJIT* jit,
        ColumnMapContext &colmapContext,
        const std::string &functionName
    )
{
#ifndef NDEBUG
    LOG_TRACE("Generating LLVM materialize function");
    auto startTime = std::chrono::steady_clock::now();
#endif

    // create an LLVM Context
    LLVMContext llvmContext;

    // create a builder for intermediate representation (IR)
    IRBuilder<> builder(llvmContext);

    auto module = LLVMCodeGenerator::getModule(jit, llvmContext, "LLVM jit module");
    module->setDataLayout(jit->getTargetMachine().createDataLayout());
    module->setTargetTriple(jit->getTargetMachine().getTargetTriple().getTriple());

    // create argument vector
    std::vector<Type*> args ({{
        Type::getInt8PtrTy(llvmContext),  // page-record-data
        Type::getInt64Ty(llvmContext),    // idx
        Type::getInt8PtrTy(llvmContext),  // dest
        Type::getInt64Ty(llvmContext),    // size
        Type::getInt32Ty(llvmContext),    // count
        Type::getInt8PtrTy(llvmContext)   // page-heap-data
    }});


    // create function
    FunctionType *funcType = FunctionType::get(Type::getVoidTy(llvmContext), args, false);
    Function *func = Function::Create(funcType, Function::ExternalLinkage, functionName, module.get());

    // create function argument names and varibles
    std::string argNames [] = {"page_record_data", "idx", "dest", "size", "count", "page_heap_data"};
    Value *argReferences [6];
    unsigned counter = 0;
    for (Argument &arg: func->args()) {
        argReferences[counter] = &arg;
        arg.setName(argNames[counter]);
        counter++;
    }

    // creeate the entry point for the function
    BasicBlock *bb = BasicBlock::Create(llvmContext, "entry", func);
    builder.SetInsertPoint(bb);

    // create the function body

    Value *recordData = argReferences[0];   // create alias for record data

    // for each for (auto fieldLength : mContext.fieldLengths())...
    Value *const1 = ConstantInt::get(llvmContext, APInt(32, 1));
    Value *constFalse = ConstantInt::getFalse(llvmContext);
    std::vector<Type *> memcpyCallVector ({{
        Type::getInt8PtrTy(llvmContext),    // dest
        Type::getInt8PtrTy(llvmContext),    // src
        Type::getInt64Ty(llvmContext),      // size
        Type::getInt32Ty(llvmContext),      // ? --> something that must be 1
        Type::getInt1Ty(llvmContext)        // ? --> something that must be false
    }});
    Function *memcpyFunc = Intrinsic::getDeclaration(module.get(), Intrinsic::memcpy, memcpyCallVector);
    for (auto size : colmapContext.fieldLengths()) {
        Value *fieldLength = ConstantInt::get(llvmContext, APInt(32, size));
        Value *src = builder.CreateGEP(recordData,
                creatMulOrShift(llvmContext, builder, argReferences[1], size));
        // prepare arguments of memcopy-call
        std::vector<Value *> memcpyValueVector ({{
            argReferences[2],
            src,
            ConstantInt::get(llvmContext, APInt(64, size)),
            const1,
            constFalse
        }});
        builder.CreateCall(memcpyFunc, memcpyValueVector);
        // dest += fieldLength
        argReferences[2] = builder.CreateAdd(argReferences[2], fieldLength);
        // recordData += count*fieldLenght
        recordData = builder.CreateGEP(recordData,
                creatMulOrShift(llvmContext, builder, argReferences[4], size));
    }

    if (colmapContext.varSizeFieldCount() != 0) {
        //        const column_map_entry_t* heap_entries = (const column_map_entry_t*) page_record_data;
        //        memcpy(dest, page_heap_data - heap_entries[idx].offset, size - FIXED_SIZE);
        Value *subtraend = builder.CreateGEP(recordData,
                creatMulOrShift(llvmContext, builder, argReferences[1], sizeof(ColumnMapHeapEntry)));
        subtraend = builder.CreateBitCast(subtraend, Type::getInt32PtrTy(llvmContext));     // need to convert from 8bit to 32bit-int-ptr
        Value *sub = builder.CreateSub(
                    ConstantInt::get(llvmContext, APInt(32, 0)),
                    builder.CreateLoad(Type::getInt32Ty(llvmContext), subtraend)
                );
        Value *src = builder.CreateGEP(argReferences[5], sub);
        // prepare arguments of memcopy-call
        std::vector<Value *> memcpyValueVector ({{
            argReferences[2],
            src,
            builder.CreateSub(
                    argReferences[3],
                    ConstantInt::get(llvmContext, APInt(32, colmapContext.fixedSize()))
            ),
            const1,
            constFalse
        }});
        builder.CreateCall(memcpyFunc, memcpyValueVector);
    }

    builder.CreateRetVoid();
    LOG_ASSERT(verifyFunction(*func), "LLVM Code Generation for ColumnMap materialize failed!");
#ifndef NDEBUG
    LOG_TRACE("Dumping LLVM Code before optimizations");
    module->dump();
#endif

    // optimize
    auto optimizer = LLVMCodeGenerator::getFunctionPassManger(module.get());
    optimizer->run(*func);  //first dump to see the difference

#ifndef NDEBUG
    LOG_TRACE("Dumping LLVM Code after optimizations");
    module->dump();
#endif

    jit->addModule(std::move(module));
    auto exprSymbol = jit->findSymbol(functionName);
    LOG_ASSERT(exprSymbol, "Couldn't find function symbol in jit module");

    auto res = reinterpret_cast<MaterializeFuncPtr>(exprSymbol.getAddress());

#ifndef NDEBUG
    auto endTime = std::chrono::steady_clock::now();
    LOG_TRACE("Generating LLVM materialize function took %1%ns", (endTime - startTime).count());
#endif

    return res;
}

void LLVMCodeGenerator::initialize() {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    InitializeNativeTargetAsmParser();
    mInitialized = true;
}

bool LLVMCodeGenerator::mInitialized = false;

} // namespace store
} // namespace tell
