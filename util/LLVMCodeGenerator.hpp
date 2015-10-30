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
#pragma once

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"

#include "LLVMJIT.hpp"

namespace tell {
namespace store {
namespace deltamain {
class ColumnMapContext;
} // namespace deltamain


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

class LLVMCodeGenerator {

public:

    static std::unique_ptr<llvm::orc::LLVMJIT> getJIT();

    static std::unique_ptr<llvm::Module> getModule(llvm::orc::LLVMJIT *jit, llvm::LLVMContext &context, std::string name);

    static std::unique_ptr<llvm::legacy::FunctionPassManager> getFunctionPassManger(llvm::Module *module);

    typedef void (*materializeFuncPtr) (
            const char* /* page data*/,
            uint64_t /* idx */,
            char* /* dest */,
            size_t /* size */,
            uint32_t /* pageCount */,
            const char* /* page record data*/
            );

    static materializeFuncPtr generate_colmap_materialize_function(
            llvm::orc::LLVMJIT* jit,
            deltamain::ColumnMapContext &colmapContext,
            const std::string &functionName
        );

private:

    static void initialize();

    static bool mInitialized;

};

} // namespace store
} // namespace tell
