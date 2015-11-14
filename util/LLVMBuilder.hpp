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

#include <tellstore/Record.hpp>

#include <llvm/IR/IRBuilder.h>

#include <cstdint>

namespace tell {
namespace store {

/**
 * @brief Helper class extending the standard LLVM IR Builder with some additional functionality
 */
class LLVMBuilder : public llvm::IRBuilder<> {
public:
    LLVMBuilder(llvm::LLVMContext& context)
            : llvm::IRBuilder<>(context) {
    }

    llvm::PointerType* getInt32PtrTy(unsigned AddrSpace = 0) {
        return llvm::Type::getInt32PtrTy(Context, AddrSpace);
    }

    /**
     * @brief Create an optimized multiplication operation with a constant
     */
    llvm::Value* createConstMul(llvm::Value* lhs, uint64_t rhs);

    /**
     * @brief Create an pointer alignment operation with a constant
     */
    llvm::Value* createPointerAlign(llvm::Value* value, uintptr_t alignment);
};

} // namespace store
} // namespace tell
