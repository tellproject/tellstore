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

#include "LLVMBuilder.hpp"

namespace tell {
namespace store {

llvm::Value* LLVMBuilder::createConstMul(llvm::Value* lhs, uint64_t rhs) {
    using namespace llvm;

    if (rhs == 0) {
        return getInt64(0);
    }
    if (rhs == 1) {
        return lhs;
    }
    if (rhs & (rhs - 1)) {
        return CreateMul(lhs, getInt64(rhs));
    }

    uint64_t log2 = 0;
    while ((rhs & 0x1u) == 0x0u) {
        rhs >>= 1;
        ++log2;
    }
    return CreateShl(lhs, getInt64(log2));
}

llvm::Value* LLVMBuilder::createPointerAlign(llvm::Value* value, uintptr_t alignment) {
    using namespace llvm;

    // -> auto result = reinterpret_cast<uintptr_t>(value);
    auto result = CreatePtrToInt(value, getInt64Ty());
    // -> result = result - 1u + alignment;
    result = CreateAdd(result, getInt64(alignment - 1u));
    // -> result = result & -alignment;
    result = CreateAnd(result, getInt64(-alignment));
    // -> recordData = reinterpret_cast<const char*>(recordDataPtr);
    result = CreateIntToPtr(result, getInt8PtrTy());

    return result;
}

} // namespace store
} // namespace tell
