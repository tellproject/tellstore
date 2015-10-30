#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "util/LLVMCodeGenerator.hpp"
#include "deltamain/colstore/ColumnMapContext.hpp"
#include "util/PageManager.hpp"

#include <gtest/gtest.h>

using namespace llvm;
using namespace llvm::orc;
using namespace tell::store;
using namespace tell::store::deltamain;

// step-by-step example of how to use LLVM to generate the following program
// for (int i = 0; i < count; ++i) if a(i) < 10: result += b(i)

TEST(LLVMCodeGeneratorTest, testA) {

    // create a jit compiler object
    auto jit = LLVMCodeGenerator::getJIT();

    // create a columnmap context
    PageManager pageManager (TELL_PAGE_SIZE);
    Schema schema;
    schema.addField(FieldType::BIGINT, crossbow::string("a"), true);
    schema.addField(FieldType::INT, crossbow::string("b"), true);
    schema.addField(FieldType::TEXT, crossbow::string("c"), true);
    Record record (schema);
    ColumnMapContext columnMapContext (pageManager, record);


//    // verify function
//    if (!verifyFunction(*cmpFunc))
//        fprintf(stderr, "Verification failed!\n");

    // Print out all of the generated code.
//    module->dump();

//    // run the code
//    auto H = jit->addModule(std::move(module));
//    auto exprSymbol = jit->findSymbol(cmpFunctionName);
//    if (!exprSymbol)
//        fprintf(stderr, "Didn't finde expression symbol!\n");
//    int (*funcPtr) (int) = (int (*)(int))(intptr_t) exprSymbol.getAddress();
//    fprintf(stderr, "Evaluates 3 to: %u --> True?\n", funcPtr(3));
//    fprintf(stderr, "Evaluates 7 to: %u --> False?\n", funcPtr(7));

//    jit->removeModule(H);
}
