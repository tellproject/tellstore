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
#include "../util/LLVMCodeGenerator.hpp"

using namespace llvm;
using namespace llvm::orc;

// step-by-step example of how to use LLVM to generate the following program
// for (int i = 0; i < count; ++i) if a(i) < 10: result += b(i)

int main() {

    // create an LLVM Context
//    LLVMContext &context = getGlobalContext();
    LLVMContext context;

    // create a builder for intermediate representation (IR)
    IRBuilder<> builder(context);

    // create a jit compiler object
    auto jit = LLVMCodeGenerator::getJIT();

    // make a module
    auto module = LLVMCodeGenerator::getModule(jit.get(), context, "coool jit module");

    // create the optimizer
    auto functionPassMgr = LLVMCodeGenerator::getFunctionPassManger(module.get());

    // create a function mult2 bottom up

//    // create the function with direct assembly code:
//    module->appendModuleInlineAsm("define i32 @mult2cmp10(i32 %x) {");
//    module->appendModuleInlineAsm("entry:");
//    module->appendModuleInlineAsm("%0 = trunc i32 %x to i31");
//    module->appendModuleInlineAsm("%ifcmp = icmp slt i31 %0, 5");
//    module->appendModuleInlineAsm("ret i1 %ifcmp");
//    module->appendModuleInlineAsm("}");

    // create the function protoype
    std::string cmpFunctionName = "mult2cmp10";
    std::vector<Type*> intarg (1, Type::getInt32Ty(context));
    FunctionType *cmpFuncType = FunctionType::get(Type::getInt32Ty(context), intarg, false);
    Function *cmpFunc = Function::Create(cmpFuncType, Function::ExternalLinkage, cmpFunctionName, module.get());
    cmpFunc->args().begin()->setName("x");
    Value *varx = cmpFunc->args().begin();

    // create an entry point for this function and link function prototype and body
    // important: this has to be done before the creation of the function body!!
    BasicBlock *bb = BasicBlock::Create(context, "entry", cmpFunc);
    builder.SetInsertPoint(bb);

    // Create blocks for the then and else cases.  Insert the 'then' block at the
    // end of the function.
//    BasicBlock *ThenBB =
//        BasicBlock::Create(context, "then", TheFunction);
//    BasicBlock *ElseBB = BasicBlock::Create(context, "else");
//    BasicBlock *MergeBB = BasicBlock::Create(context, "ifcont");
//    builder.CreateCondBr(CondV, ThenBB, ElseBB);

    // create the comparison function body and add it as return value
    Value *const2 = ConstantInt::get(context, APInt(32, 2)); // 32 bits, value 2
    Value *const10 = ConstantInt::get(context, APInt(32, 10));
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
    fprintf(stderr, "Evaluates 3 to: %u --> True?\n", funcPtr(3));
    fprintf(stderr, "Evaluates 7 to: %u --> False?\n", funcPtr(7));

    jit->removeModule(H);

    return 0;
}
