#include "llvm/ADT/STLExtras.h"
#include "llvm/Analysis/Passes.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
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

int main() {

  // create a builder for intermediate representation (IR)
  IRBuilder<> builder(getGlobalContext());

  // create a jit compiler object
  std::unique_ptr<KaleidoscopeJIT> jit = llvm::make_unique<KaleidoscopeJIT>(); //TODO: this call fails, why?!

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
  functionPassMgr->doInitialization();

  // create a function mult2 bottom up

  // create the function protoype
  std::vector<Type*> intarg (1, Type::getInt32Ty(getGlobalContext()));
  FunctionType *fType = FunctionType::get(Type::getInt32Ty(getGlobalContext()), intarg, false);
  Function *func = Function::Create(fType, Function::ExternalLinkage, "mult2add3", module.get());
  func->args().begin()->setName("x");
  Value *varx = func->args().begin();

  // create an entry point for this function and link function prototype and body
  // important: this has to be done before the creation of the function body!!
  BasicBlock *bb = BasicBlock::Create(getGlobalContext(), "entry", func);
  builder.SetInsertPoint(bb);

  // create the function body and add it as return value
  Value *const2 = ConstantInt::get(getGlobalContext(), APInt(32, 2)); // 32 bits, value 2
  Value *const3 = ConstantInt::get(getGlobalContext(), APInt(32, 3));
  Value *mult2 = builder.CreateMul(const2, varx, "tmpMult");
  Value *add3 = builder.CreateAdd(const3, mult2, "tmpAdd");
  builder.CreateRet(add3);

  // verify function
  if (!verifyFunction(*func))
      fprintf(stderr, "Verification failed!\n");

  // Print out all of the generated code.
  module->dump();


  // optimize the function with the created optimizer (function pass manager)
  functionPassMgr->run(*func);
  module->dump();

  // run the code
  auto H = jit->addModule(std::move(module));
  auto exprSymbol = jit->findSymbol("mult2add3");
  if (!exprSymbol)
      fprintf(stderr, "Didn't finde expression symbol!\n");
  int (*funcPtr) (int) = (int (*)(int))(intptr_t) exprSymbol.getAddress();
  fprintf(stderr, "Evaluates 7 to: %d\n", funcPtr(7));

  jit->removeModule(H);

  return 0;
}
