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
#include <sstream>
#include <string>

namespace tell {
namespace store {

class ScanQuery;

namespace deltamain {

class ColumnMapContext;

/**
 * @brief Helper class creating the column map aggregation function
 */
class LLVMColumnMapAggregationBuilder : private FunctionBuilder {
public:
    using Signature = uint32_t (*) (
            const char* /* page */,
            uint64_t /* startIdx */,
            uint64_t /* endIdx */,
            const char* /* result */,
            char* /* dest */);

    static const std::string FUNCTION_NAME;

    static void createFunction(const ColumnMapContext& context, llvm::Module& module, llvm::TargetMachine* target,
            uint32_t index, ScanQuery* query) {
        LLVMColumnMapAggregationBuilder builder(context, module, target, index);
        builder.build(query);
    }

    static std::string createFunctionName(uint32_t index) {
        std::stringstream ss;
        ss << FUNCTION_NAME << index;
        return ss.str();
    }

private:
    static constexpr size_t page = 0;
    static constexpr size_t startIdx = 1;
    static constexpr size_t endIdx = 2;
    static constexpr size_t result = 3;
    static constexpr size_t dest = 4;

    static llvm::Type* buildReturnTy(llvm::LLVMContext& context) {
        return llvm::Type::getInt32Ty(context);
    }

    static std::vector<std::pair<llvm::Type*, crossbow::string>> buildParamTy(llvm::LLVMContext& context) {
        return {
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "page" },
            { llvm::Type::getInt64Ty(context), "startIdx" },
            { llvm::Type::getInt64Ty(context), "endIdx" },
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "result" },
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "dest" }
        };
    }

    LLVMColumnMapAggregationBuilder(const ColumnMapContext& context, llvm::Module& module, llvm::TargetMachine* target,
            uint32_t index);

    void build(ScanQuery* query);

    const ColumnMapContext& mContext;

    llvm::StructType* mMainPageStructTy;

    uint64_t mRegisterWidth;
};

} // namespace deltamain
} // namespace store
} // namespace tell
