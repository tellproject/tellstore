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

#include "LLVMColumnMapUtils.hpp"

#include "ColumnMapPage.hpp"

namespace tell {
namespace store {
namespace deltamain {

llvm::StructType* getColumnMapMainPageTy(llvm::LLVMContext& context) {
    static_assert(sizeof(ColumnMapMainPage) == 16, "Size of ColumnMapHeapEntry must be 16");
    static_assert(offsetof(ColumnMapMainPage, count) == 0, "Offset of count must be 0");
    static_assert(offsetof(ColumnMapMainPage, headerOffset) == 4, "Offset of headerOffset must be 4");
    static_assert(offsetof(ColumnMapMainPage, fixedOffset) == 8, "Offset of fixedOffset must be 8");
    static_assert(offsetof(ColumnMapMainPage, variableOffset) == 12, "Offset of variableOffset must be 12");
    return llvm::StructType::get(context, {
        llvm::Type::getInt32Ty(context),    // count
        llvm::Type::getInt32Ty(context),    // headerOffset
        llvm::Type::getInt32Ty(context),    // fixedOffset
        llvm::Type::getInt32Ty(context)     // variableOffset
    });
}

llvm::StructType* getColumnMapHeapEntriesTy(llvm::LLVMContext& context) {
    static_assert(sizeof(ColumnMapHeapEntry) == 8, "Size of ColumnMapHeapEntry must be 8");
    static_assert(offsetof(ColumnMapHeapEntry, offset) == 0, "Offset of offset must be 0");
    static_assert(offsetof(ColumnMapHeapEntry, prefix) == 4, "Offset of prefix must be 4");
    return llvm::StructType::get(context, {
        llvm::Type::getInt32Ty(context),    // offset
        llvm::Type::getInt32Ty(context)     // prefix
    });
}

} // namespace deltamain
} // namespace store
} // namespace tell
