#include "llvm/ADT/STLExtras.h"
#include "llvm/Analysis/Passes.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Vectorize.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/Bitcode/BitcodeWriterPass.h"
#include <cctype>
#include <cstdio>
#include <map>
#include <string>
#include <vector>
#include "include/KaleidoscopeJIT.h"

using namespace llvm;
using namespace llvm::orc;

// step-by-step example of how to use LLVM to generate the following program
// for (int i = 0; i < count; ++i) if a(i) < 10: result += b(i)

int main() {

    // initialize target machine and machine-specific settings
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    InitializeNativeTargetAsmParser();

    // create a builder for intermediate representation (IR)
    IRBuilder<> builder(getGlobalContext());

    // create a jit compiler object
    std::unique_ptr<KaleidoscopeJIT> jit = llvm::make_unique<KaleidoscopeJIT>();

    // make a module, which holds all the code and the builder that uses it to create code
    std::unique_ptr<Module> module = llvm::make_unique<Module>("my cool jit", getGlobalContext());
    module->setDataLayout(jit->getTargetMachine().createDataLayout());

    // create a function pass manager attached to the module with some optimizations
    std::unique_ptr<legacy::FunctionPassManager> functionPassMgr = llvm::make_unique<legacy::FunctionPassManager>(module.get());
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
//    functionPassMgr->add(createBBVectorizePass()); --> TODO: resolve linker problems
    // add loop vectorization
//    functionPassMgr->add(createLoopVectorizePass()); --> TODO: resolve linker problems
    // initialize function pass manager
    functionPassMgr->doInitialization();

    // create a function mult2 bottom up

    // create the function protoype
    std::string cmpFunctionName = "mult2cmp10";
    std::vector<Type*> intarg (1, Type::getInt32Ty(getGlobalContext()));
    FunctionType *cmpFuncType = FunctionType::get(Type::getInt32Ty(getGlobalContext()), intarg, false);
    Function *cmpFunc = Function::Create(cmpFuncType, Function::ExternalLinkage, cmpFunctionName, module.get());
    cmpFunc->args().begin()->setName("x");
    Value *varx = cmpFunc->args().begin();

    // create an entry point for this function and link function prototype and body
    // important: this has to be done before the creation of the function body!!
    BasicBlock *bb = BasicBlock::Create(getGlobalContext(), "entry", cmpFunc);
    builder.SetInsertPoint(bb);

    // Create blocks for the then and else cases.  Insert the 'then' block at the
    // end of the function.
//    BasicBlock *ThenBB =
//        BasicBlock::Create(getGlobalContext(), "then", TheFunction);
//    BasicBlock *ElseBB = BasicBlock::Create(getGlobalContext(), "else");
//    BasicBlock *MergeBB = BasicBlock::Create(getGlobalContext(), "ifcont");
//    builder.CreateCondBr(CondV, ThenBB, ElseBB);

    // create the comparison function body and add it as return value
    Value *const2 = ConstantInt::get(getGlobalContext(), APInt(32, 2)); // 32 bits, value 2
    Value *const10 = ConstantInt::get(getGlobalContext(), APInt(32, 10));
    Value *mult2 = builder.CreateMul(const2, varx, "tmpMult");
    Value *compOp = builder.CreateICmpSLT(mult2, const10, "ifcmp");
    builder.CreateRet(compOp);

    // verify function
    if (!verifyFunction(*cmpFunc))
        fprintf(stderr, "Verification failed!\n");

    // Print out all of the generated code.
    module->dump();


    // optimize the function with the created optimizer (function pass manager)
    functionPassMgr->run(*cmpFunc);
    module->dump();

    // run the code
    auto H = jit->addModule(std::move(module));
    auto exprSymbol = jit->findSymbol(cmpFunctionName);
    if (!exprSymbol)
        fprintf(stderr, "Didn't finde expression symbol!\n");
    int (*funcPtr) (int) = (int (*)(int))(intptr_t) exprSymbol.getAddress();
    fprintf(stderr, "Evaluates 3 to: %u\n --> True?", funcPtr(3));
    fprintf(stderr, "Evaluates 7 to: %u\n --> False?", funcPtr(7));

    jit->removeModule(H);

    return 0;
}
