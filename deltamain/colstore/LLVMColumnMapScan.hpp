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
#include <util/LLVMScan.hpp>

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>

namespace tell {
namespace store {
namespace deltamain {

class ColumnMapContext;

/**
 * @brief Helper class creating the column map scan function
 */
class LLVMColumnMapScanBuilder : private FunctionBuilder {
public:
    static const std::string FUNCTION_NAME;

    static void createFunction(const ColumnMapContext& context, llvm::Module& module, llvm::TargetMachine* target,
            const ScanAST& scanAst) {
        LLVMColumnMapScanBuilder builder(context, module, target);
        builder.buildScan(scanAst);
    }

private:
    static constexpr size_t keyData = 0;
    static constexpr size_t validFromData = 1;
    static constexpr size_t validToData = 2;
    static constexpr size_t page = 3;
    static constexpr size_t startIdx = 4;
    static constexpr size_t endIdx = 5;
    static constexpr size_t resultData = 6;

    static llvm::Type* buildReturnTy(llvm::LLVMContext& context) {
        return llvm::Type::getVoidTy(context);
    }

    static std::vector<std::pair<llvm::Type*, crossbow::string>> buildParamTy(llvm::LLVMContext& context) {
        return {
            { llvm::Type::getInt64Ty(context)->getPointerTo(), "keyData" },
            { llvm::Type::getInt64Ty(context)->getPointerTo(), "validFromData" },
            { llvm::Type::getInt64Ty(context)->getPointerTo(), "validToData" },
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "page" },
            { llvm::Type::getInt64Ty(context), "startIdx" },
            { llvm::Type::getInt64Ty(context), "endIdx" },
            { llvm::Type::getInt8Ty(context)->getPointerTo(), "resultData" }
        };
    }

    LLVMColumnMapScanBuilder(const ColumnMapContext& context, llvm::Module& module, llvm::TargetMachine* target);

    void buildScan(const ScanAST& scanAst);

    void buildField(const FieldAST& field);

    llvm::Value* buildFixedFieldEvaluation(llvm::Value* recordPtr, llvm::Value* startIdx, uint64_t vectorSize,
            std::vector<uint8_t>& conjunctsGenerated, const FieldAST& fieldAst, const llvm::Twine& name);

    void buildQuery(bool needsKey, const std::vector<QueryAST>& queries);

    llvm::Value* buildQueryEvaluation(llvm::Value* startIdx, uint64_t vectorSize, bool needsKey,
            const std::vector<QueryAST>& queries, const llvm::Twine& name);

    void buildResult(const std::vector<QueryAST>& queries);

    llvm::Value* buildConjunctMerge(llvm::Value* startIdx, uint64_t vectorSize, uint32_t src, uint32_t dest,
            const llvm::Twine& name);

    const ColumnMapContext& mContext;

    llvm::StructType* mMainPageStructTy;

    llvm::Value* mMainPage;
    llvm::Value* mCount;
    llvm::Value* mFixedData;

    uint64_t mRegisterWidth;

    std::vector<uint8_t> mVectorConjunctsGenerated;
    std::vector<uint8_t> mScalarConjunctsGenerated;
};

} // namespace deltamain
} // namespace store
} // namespace tell
