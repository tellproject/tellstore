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

#include <util/LLVMBuilder.hpp>

#include <crossbow/string.hpp>

#include <cstddef>
#include <string>

namespace tell {
namespace store {

class Record;
class ScanQuery;

/**
 * @brief Helper class creating the row aggregation function
 */
class LLVMRowAggregationBuilder : private FunctionBuilder {
public:
    using Signature = uint32_t (*) (
            const char* /* src */,
            uint32_t /* size */,
            char* /* dest */);

    static void createFunction(const Record& record, llvm::Module& module, llvm::TargetMachine* target,
            const std::string& name, ScanQuery* query) {
        LLVMRowAggregationBuilder builder(record, module, target, name);
        builder.build(query);
    }

private:
    static constexpr size_t src = 0;
    static constexpr size_t size = 1;
    static constexpr size_t dest = 2;

    static llvm::Type* buildReturnTy(llvm::LLVMContext& context) {
        return llvm::Type::getInt32Ty(context);
    }

    static std::vector<std::pair<llvm::Type*, crossbow::string>> buildParamTy(llvm::LLVMContext& context) {
        return {
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "src" },
            { llvm::Type::getInt32Ty(context), "size" },
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "dest" }
        };
    }

    LLVMRowAggregationBuilder(const Record& record, llvm::Module& module, llvm::TargetMachine* target,
            const std::string& name);

    void build(ScanQuery* query);

    const Record& mRecord;
};

} // namespace store
} // namespace tell
