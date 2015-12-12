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

#include <cstdint>
#include <string>
#include <vector>

namespace tell {
namespace store {

class ScanAST;

/**
 * @brief Helper class creating the row store scan function
 */
class LLVMRowScanBuilder : private FunctionBuilder {
public:
    static const std::string FUNCTION_NAME;

    static void createFunction(llvm::Module& module, llvm::TargetMachine* target, const ScanAST& scanAst) {
        LLVMRowScanBuilder builder(module, target);
        builder.buildScan(scanAst);
    }

private:
    static constexpr size_t key = 0;
    static constexpr size_t validFrom = 1;
    static constexpr size_t validTo = 2;
    static constexpr size_t recordData = 3;
    static constexpr size_t resultData = 4;

    static llvm::Type* buildReturnTy(llvm::LLVMContext& context) {
        return llvm::Type::getVoidTy(context);
    }

    static std::vector<std::pair<llvm::Type*, crossbow::string>> buildParamTy(llvm::LLVMContext& context) {
        return {
            { llvm::Type::getInt64Ty(context), "key" },
            { llvm::Type::getInt64Ty(context), "validFrom" },
            { llvm::Type::getInt64Ty(context), "validTo" },
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "recordData" },
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "resultData" }
        };
    }

    LLVMRowScanBuilder(llvm::Module& module, llvm::TargetMachine* target);

    void buildScan(const ScanAST& scanAst);

    std::vector<uint8_t> mConjunctsGenerated;
};

} //namespace store
} //namespace tell
