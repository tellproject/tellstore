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

#include <util/LLVMJIT.hpp>
#include <util/ScanQuery.hpp>

#include <crossbow/non_copyable.hpp>

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>

#include <cstdint>
#include <memory>
#include <vector>

struct LLVMScanQueryProcessor {
    char* bufferPos;

    const char* bufferEnd;

    size_t length;
};

extern "C" void queryProcessorAcquireBuffer(LLVMScanQueryProcessor* data);


namespace tell {
namespace store {

class Record;

class LLVMScanBase {
public:
    template <typename Fun>
    Fun findFunction(const std::string& name) {
        return mCompiler.findFunction<Fun>(name);
    }

protected:
    LLVMScanBase();

    ~LLVMScanBase();

    void finalizeScan();

    LLVMJIT mCompiler;

    llvm::LLVMContext mCompilerContext;

    llvm::Module mCompilerModule;

    LLVMJIT::ModuleHandle mCompilerHandle;
};

class LLVMRowScanBase : public LLVMScanBase {
public:
    using RowScanFun = void (*) (uint64_t /* key */, uint64_t /* validFrom */, uint64_t /* validTo */,
            const char* /* recordData */, char* /* destData */);

    using RowMaterializeFun = uint32_t (*) (const char* /* srcData */, uint32_t /* length */, char* /* destData */);

protected:
    LLVMRowScanBase(const Record& record, std::vector<ScanQuery*> queries);

    ~LLVMRowScanBase() = default;

    void finalizeRowScan();

    std::vector<ScanQuery*> mQueries;

    RowScanFun mRowScanFun;

    std::vector<RowMaterializeFun> mRowMaterializeFuns;

    uint32_t mNumConjuncts;

private:
    void prepareRowScanFunction(const Record& record);

    void prepareRowProjectionFunction(const Record& srcRecord, ScanQuery* query, uint32_t index);

    void prepareRowAggregationFunction(const Record& srcRecord, ScanQuery* query, uint32_t index);
};

class LLVMRowScanProcessorBase {
protected:
    LLVMRowScanProcessorBase(const Record& record, const std::vector<ScanQuery*>& queries,
            LLVMRowScanBase::RowScanFun rowScanFunc,
            const std::vector<LLVMRowScanBase::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts);

    ~LLVMRowScanProcessorBase() = default;

    /**
     * @brief Process the record with all associated scan processors
     *
     * Checks the tuple against the combined query buffer.
     *
     * @param key Key of the tuple
     * @param validFrom Valid-From version of the tuple
     * @param validTo Valid-To version of the tuple
     * @param data Pointer to the tuple's data
     * @param length Length of the tuple
     */
    void processRowRecord(uint64_t key, uint64_t validFrom, uint64_t validTo, const char* data, uint32_t length);

    const Record& mRecord;

    std::vector<ScanQueryProcessor> mQueries;

    LLVMRowScanBase::RowScanFun mRowScanFun;

    std::vector<LLVMRowScanBase::RowMaterializeFun> mRowMaterializeFuns;

    uint32_t mNumConjuncts;

    std::vector<char> mResult;
};

} //namespace store
} //namespace tell
