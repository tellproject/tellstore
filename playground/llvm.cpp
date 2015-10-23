#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include <cctype>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

using namespace llvm;

int main() {

  // Make the module, which holds all the code and the builder that uses it to create code
  std::unique_ptr<Module> module = llvm::make_unique<Module>("my cool jit", getGlobalContext());
  IRBuilder<> builder(getGlobalContext());

  // create a function mult2 bottom up

  // create function protoype
  std::vector<Type*> intarg (1, Type::getInt32Ty(getGlobalContext()));
  FunctionType *fType = FunctionType::get(Type::getInt32Ty(getGlobalContext()), intarg, false);
  Function *func = Function::Create(fType, Function::ExternalLinkage, "mult2", module.get());
  func->args().begin()->setName("x");
  Value *varx = func->args().begin();

  // create function body
  Value *const2 = ConstantInt::get(getGlobalContext(), APInt(2, 4));
  Value *mult2 = builder.CreateMul(const2, varx);

  // create an entry point for this function and link function prototype and body
  BasicBlock *bb = BasicBlock::Create(getGlobalContext(), "entry", func);
  builder.SetInsertPoint(bb);
  builder.CreateRet(mult2);

  // verify function
  if (!verifyFunction(*func))
      fprintf(stderr, "Verification failed!\n");
  else
      fprintf(stderr, "Verification succeeded!\n");

  // Print out all of the generated code.
  module->dump();

  return 0;
}
