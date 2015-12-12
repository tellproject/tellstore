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
#include <util/LLVMJIT.hpp>
#include <util/ScanQuery.hpp>

#include <crossbow/non_copyable.hpp>

#include <llvm/IR/InstrTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace tell {
namespace store {

class Record;

/**
 * @brief AST node representing a predicate on a fixed size field
 */
struct FixedPredicateAST {
    /// The value the predicate must match
    llvm::Constant* value;

    /// Predicate of the comparison
    llvm::CmpInst::Predicate predicate;

    /// Whether the value is a float or an integer
    bool isFloat;
};

/**
 * @brief AST node representing a predicate on a variable size field
 */
struct VariablePredicateAST {
    /// Total size of the data (including the prefix)
    uint32_t size;

    /// First bytes of the data the predicate must match
    char prefix[4];

    /// Data the predicate must match
    llvm::GlobalVariable* value;

    /// Whether a like matches on the prefix or postfix (only valid for like predicate)
    bool isPrefixLike;
};

/**
 * @brief AST node representing a predicate on a field
 */
struct PredicateAST {
    PredicateAST(PredicateType _type, uint32_t _conjunct)
            : type(_type),
              conjunct(_conjunct) {
    }

    PredicateType type;

    /// Conjunct index in the result vector the predicate is attached to
    uint32_t conjunct;

    union {
        /// The value the predicate must match in case the field fix sized
        /// Not active in case the predicate type matches on the null status
        FixedPredicateAST fixed;

        /// The value the predicate must match in case the field is variable sized
        /// Not active in case the predicate type matches on the null status
        VariablePredicateAST variable;
    };
};

/**
 * @brief AST node representing a number of predicates on a field
 */
struct FieldAST {
    uint16_t id;
    FieldType type;
    bool isNotNull;
    bool needsValue;
    bool isFixedSize;

    uint32_t offset;
    uint32_t alignment;
    uint32_t size;

    std::vector<PredicateAST> predicates;
};

/**
 * @brief AST node representing a single query
 */
struct QueryAST {
    /// Base version of the snapshot
    uint64_t baseVersion;

    /// Version of the snapshot
    uint64_t version;

    /// Offset to the first non-result conjunct of this query
    uint32_t conjunctOffset;

    /// Number of conjuncts in the query
    uint32_t numConjunct;

    /// Overall number of partitions (or 0 if no partitioning)
    uint32_t partitionModulo;

    /// Partition the query is interested in
    uint32_t partitionNumber;
};

/**
 * @brief Root AST node of a scan
 */
struct ScanAST {
    ScanAST()
            : numConjunct(0),
              needsKey(false) {
    }

    /// Number of conjuncts in total
    uint32_t numConjunct;

    /// Whether any scan has a partition on it (and as such the key is needed)
    bool needsKey;

    std::map<uint16_t, FieldAST> fields;

    std::vector<QueryAST> queries;
};

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

    ScanAST mScanAst;

    RowScanFun mRowScanFun;

    std::vector<RowMaterializeFun> mRowMaterializeFuns;

private:
    void buildScanAST(const Record& record);

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
