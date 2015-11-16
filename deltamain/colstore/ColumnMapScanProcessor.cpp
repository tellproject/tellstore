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

#include "ColumnMapScanProcessor.hpp"

#include "ColumnMapContext.hpp"
#include "ColumnMapPage.hpp"
#include "ColumnMapRecord.hpp"

#include <deltamain/Table.hpp>

#include <util/LLVMBuilder.hpp>

#include <llvm/ADT/ArrayRef.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Vectorize.h>

#include <array>

namespace tell {
namespace store {
namespace deltamain {
namespace {

static const std::string gColumnScanFunctionName = "columnScan";

static const std::array<std::string, 7> gColumnScanParamNames = {{
    "keyData",
    "validFromData",
    "validToData",
    "recordData",
    "heapData",
    "count",
    "resultData"
}};

struct QueryState {
    QueryState(const ScanQuery* query, uint32_t _conjunctOffset)
            : queryReader(query->selection(), query->selectionLength()),
              snapshot(query->snapshot()),
              conjunctOffset(_conjunctOffset),
              numConjunct(0u),
              partitionModulo(0u),
              partitionNumber(0u),
              currentColumn(0u) {
    }

    crossbow::buffer_reader queryReader;

    const commitmanager::SnapshotDescriptor* snapshot;

    uint32_t conjunctOffset;

    uint32_t numConjunct;

    uint32_t partitionModulo;

    uint32_t partitionNumber;

    uint16_t currentColumn;
};

} // anonymous namespace

ColumnMapScan::ColumnMapScan(Table<ColumnMapContext>* table, std::vector<ScanQuery*> queries)
        : LLVMRowScanBase(table->record(), std::move(queries)),
          mTable(table),
          mColumnScanFun(nullptr) {
    prepareColumnScanFunction(table->record());

    finalizeRowScan();
    mColumnScanFun = mCompiler.findFunction<ColumnScanFun>(gColumnScanFunctionName);
}

std::vector<std::unique_ptr<ColumnMapScanProcessor>> ColumnMapScan::startScan(size_t numThreads) {
    return mTable->startScan(numThreads, mQueries, mColumnScanFun, mRowScanFun, mNumConjuncts);
}

void ColumnMapScan::prepareColumnScanFunction(const Record& record) {
    using namespace llvm;

    static constexpr size_t keyData = 0;
    static constexpr size_t validFromData = 1;
    static constexpr size_t validToData = 2;
    static constexpr size_t recordData = 3;
    static constexpr size_t heapData = 4;
    static constexpr size_t count = 5;
    static constexpr size_t resultData = 6;

    LLVMBuilder builder(mCompilerContext);

    // Create function
    auto funcType = FunctionType::get(builder.getVoidTy(), {
            builder.getInt64PtrTy(),    // keyData
            builder.getInt64PtrTy(),    // validFromData
            builder.getInt64PtrTy(),    // validToData
            builder.getInt8PtrTy(),     // recordData
            builder.getInt8PtrTy(),     // heapData
            builder.getInt64Ty(),       // count
            builder.getInt8PtrTy()      // destData
    }, false);
    auto func = Function::Create(funcType, Function::ExternalLinkage, gColumnScanFunctionName, &mCompilerModule);

    // Set arguments names
    std::array<Value*, 7> args;
    {
        decltype(gColumnScanParamNames.size()) idx = 0;
        for (auto iter = func->arg_begin(); idx != gColumnScanParamNames.size(); ++iter, ++idx) {
            iter->setName(gColumnScanParamNames[idx]);
            args[idx] = iter.operator ->();
        }
    }

    // Set noalias hints (data pointers are not allowed to overlap)
    func->setDoesNotAlias(1);
    func->setOnlyReadsMemory(1);
    func->setDoesNotAlias(2);
    func->setOnlyReadsMemory(2);
    func->setDoesNotAlias(3);
    func->setOnlyReadsMemory(3);
    func->setDoesNotAlias(4);
    func->setOnlyReadsMemory(4);
    func->setDoesNotAlias(5);
    func->setOnlyReadsMemory(5);
    func->setDoesNotAlias(7);

    // Build function
    auto bb = BasicBlock::Create(mCompilerContext, "entry", func);
    builder.SetInsertPoint(bb);

    if (mQueries.size() == 0) {
        builder.CreateRetVoid();
        return;
    }
    auto loopCounterAlloc = builder.CreateAlloca(builder.getInt64Ty(), nullptr, "i");

    std::vector<QueryState> queryBuffers;
    queryBuffers.reserve(mQueries.size());

    uint32_t numConjuncts = mQueries.size();
    for (auto q : mQueries) {
        queryBuffers.emplace_back(q, numConjuncts);

        auto& state = queryBuffers.back();
        auto& queryReader = state.queryReader;
        auto numColumns = queryReader.read<uint32_t>();

        state.numConjunct = queryReader.read<uint32_t>();
        state.partitionModulo = queryReader.read<uint32_t>();
        state.partitionNumber = queryReader.read<uint32_t>();
        if (numColumns != 0) {
            state.currentColumn = queryReader.read<uint16_t>();
        } else {
            state.currentColumn = std::numeric_limits<uint16_t>::max();
        }

        numConjuncts += state.numConjunct;
    }
    LOG_ASSERT(mNumConjuncts == numConjuncts, "Number of conjuncts differ from row scan function");

    auto currentColumn = std::numeric_limits<uint16_t>::max();
    decltype(queryBuffers.size()) nextQueryIdx = 0;
    while (true) {
        // Find the next query with the nearest column
        for (decltype(queryBuffers.size()) i = 0; i < queryBuffers.size(); ++i) {
            auto& state = queryBuffers.at(i);
            if (state.currentColumn < currentColumn) {
                currentColumn = state.currentColumn;
                nextQueryIdx = i;
            }
        }
        if (currentColumn == std::numeric_limits<uint16_t>::max()) {
            break;
        }

        // -> auto src = page->recordData() + page->count * mFieldOffsets[idx];
        auto& field = record.getFieldMeta(currentColumn).first;
        auto fieldOffset = record.getFieldMeta(currentColumn).second;
        auto fieldAlignment = field.alignOf();
        auto src = (fieldOffset == 0
                ? args[recordData]
                : builder.CreateInBoundsGEP(args[recordData], builder.createConstMul(args[count], fieldOffset)));
        src = builder.CreateBitCast(src, builder.getFieldPtrTy(field.type()));

        // i = 0u;
        builder.CreateAlignedStore(builder.getInt64(0u), loopCounterAlloc, 8u);

        // Loop generation
        auto loopBB = BasicBlock::Create(mCompilerContext, "col." + Twine(currentColumn), func);
        builder.CreateBr(loopBB);
        builder.SetInsertPoint(loopBB);

        auto loopCounter = builder.CreateAlignedLoad(loopCounterAlloc, 8u);

        auto fieldValue = builder.CreateAlignedLoad(builder.CreateInBoundsGEP(src, loopCounter), fieldAlignment);

        for (decltype(queryBuffers.size()) i = nextQueryIdx; i < queryBuffers.size(); ++i) {
            auto& state = queryBuffers.at(i);
            if (state.currentColumn != currentColumn) {
                continue;
            }
            auto& queryReader = state.queryReader;

            auto numPredicates = queryReader.read<uint16_t>();
            queryReader.advance(4);

            for (decltype(numPredicates) j = 0; j < numPredicates; ++j) {
                auto predicateType = queryReader.read<PredicateType>();
                auto conjunctPosition = state.conjunctOffset + queryReader.read<uint8_t>();

                bool isFloat;
                Value* compareValue;
                switch (field.type()) {
                case FieldType::SMALLINT: {
                    isFloat = false;
                    compareValue = builder.getInt16(queryReader.read<int16_t>());
                    queryReader.advance(4);
                } break;

                case FieldType::INT: {
                    isFloat = false;
                    queryReader.advance(2);
                    compareValue = builder.getInt32(queryReader.read<int32_t>());
                } break;

                case FieldType::BIGINT: {
                    isFloat = false;
                    queryReader.advance(6);
                    compareValue = builder.getInt64(queryReader.read<int64_t>());
                } break;

                case FieldType::FLOAT: {
                    isFloat = true;
                    queryReader.advance(2);
                    compareValue = builder.getFloat(queryReader.read<float>());
                } break;

                case FieldType::DOUBLE: {
                    isFloat = true;
                    queryReader.advance(6);
                    compareValue = builder.getDouble(queryReader.read<double>());
                } break;

                default: {
                    LOG_ASSERT(false, "Only fixed size fields are supported (right now)");
                    isFloat = false;
                    compareValue = nullptr;
                } break;
                }

                CmpInst::Predicate predicate;
                switch (predicateType) {
                case PredicateType::EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_OEQ : CmpInst::ICMP_EQ);
                } break;

                case PredicateType::NOT_EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_ONE : CmpInst::ICMP_NE);
                } break;

                case PredicateType::LESS: {
                    predicate = (isFloat ? CmpInst::FCMP_OLT : CmpInst::ICMP_SLT);
                } break;

                case PredicateType::LESS_EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_OLE : CmpInst::ICMP_SLE);
                } break;

                case PredicateType::GREATER: {
                    predicate = (isFloat ? CmpInst::FCMP_OGT : CmpInst::ICMP_SGT);
                } break;

                case PredicateType::GREATER_EQUAL: {
                    predicate = (isFloat ? CmpInst::FCMP_OGE : CmpInst::ICMP_SGE);
                } break;

                default: {
                    LOG_ASSERT(false, "Predicate not supported (right now)");
                    predicate = (isFloat ? CmpInst::BAD_FCMP_PREDICATE : CmpInst::BAD_ICMP_PREDICATE);
                } break;
                }

                auto comp = (isFloat
                        ? builder.CreateFCmp(predicate, fieldValue, compareValue)
                        : builder.CreateICmp(predicate, fieldValue, compareValue));
                auto compResult = builder.CreateZExt(comp, builder.getInt8Ty());

                auto conjunctIndex = builder.CreateAdd(
                        builder.createConstMul(args[count], conjunctPosition),
                        loopCounter);
                auto conjunctElement = builder.CreateInBoundsGEP(args[resultData], conjunctIndex);

                // -> res[i] = res[i] | comp;
                auto res = builder.CreateOr(builder.CreateLoad(conjunctElement), compResult);
                builder.CreateStore(res, conjunctElement);
            }

            if (queryReader.exhausted()) {
                state.currentColumn = std::numeric_limits<uint16_t>::max();
            } else {
                state.currentColumn = queryReader.read<uint16_t>();
            }
        }

        // -> i += 1;
        auto nextVar = builder.CreateAdd(loopCounter, builder.getInt64(1));
        builder.CreateAlignedStore(nextVar, loopCounterAlloc, 8u);

        // i < page->count
        auto endCond = builder.CreateICmp(ICmpInst::ICMP_ULT, nextVar, args[count]);

        // Create the loop
        auto afterBB = BasicBlock::Create(mCompilerContext, "endcol." + Twine(currentColumn), func);
        builder.CreateCondBr(endCond, loopBB, afterBB);
        builder.SetInsertPoint(afterBB);

        currentColumn = std::numeric_limits<uint16_t>::max();
    }

    {
        // i = 0u;
        builder.CreateAlignedStore(builder.getInt64(0u), loopCounterAlloc, 8u);

        auto loopBB = BasicBlock::Create(mCompilerContext, "check", func);
        builder.CreateBr(loopBB);
        builder.SetInsertPoint(loopBB);

        auto loopCounter = builder.CreateAlignedLoad(loopCounterAlloc, 8u);

        for (decltype(queryBuffers.size()) i = 0; i < queryBuffers.size(); ++i) {
            auto& state = queryBuffers.at(i);
            auto snapshot = state.snapshot;

            // Evaluate validFrom <= version && validTo > baseVersion
            auto validFrom = builder.CreateAlignedLoad(builder.CreateInBoundsGEP(args[validFromData], loopCounter), 8u);
            auto validTo = builder.CreateAlignedLoad(builder.CreateInBoundsGEP(args[validToData], loopCounter), 8u);

            auto res = builder.CreateAnd(
                    builder.CreateICmp(CmpInst::ICMP_ULE, validFrom, builder.getInt64(snapshot->version())),
                    builder.CreateICmp(CmpInst::ICMP_UGT, validTo, builder.getInt64(snapshot->baseVersion())));

            // Evaluate partitioning key % partitionModulo == partitionNumber
            if (state.partitionModulo != 0u) {
                auto key = builder.CreateAlignedLoad(builder.CreateInBoundsGEP(args[keyData], loopCounter), 8u);
                auto compResult = builder.CreateICmp(CmpInst::ICMP_EQ,
                        builder.createConstMod(key, state.partitionModulo),
                        builder.getInt64(state.partitionNumber));
                res = builder.CreateAnd(res, compResult);
            }
            res = builder.CreateZExt(res, builder.getInt8Ty());

            Value* conjunctIndex = loopCounter;
            if (i > 0) {
                conjunctIndex = builder.CreateAdd(conjunctIndex, builder.createConstMul(args[count], i));
            }
            builder.CreateStore(res, builder.CreateInBoundsGEP(args[resultData], conjunctIndex));
        }
        // -> i += 1;
        auto nextVar = builder.CreateAdd(loopCounter, builder.getInt64(1));
        builder.CreateAlignedStore(nextVar, loopCounterAlloc, 8u);

        // i < page->count
        auto endCond = builder.CreateICmp(ICmpInst::ICMP_ULT, nextVar, args[count]);

        // Create the loop
        auto afterBB = BasicBlock::Create(mCompilerContext, "endcheck", func);
        builder.CreateCondBr(endCond, loopBB, afterBB);
        builder.SetInsertPoint(afterBB);
    }

    for (decltype(queryBuffers.size()) i = 0; i < queryBuffers.size(); ++i) {
        auto& state = queryBuffers.at(i);
        if (state.numConjunct == 0u) {
            continue;
        }

        for (decltype(state.numConjunct) j = state.numConjunct - 1u; j > 0u; --j) {
            auto conjunctPosition = state.conjunctOffset + j - 1u;

            // i = 0u;
            builder.CreateAlignedStore(builder.getInt64(0u), loopCounterAlloc, 8u);

            auto loopBB = BasicBlock::Create(mCompilerContext, "conj." + Twine(conjunctPosition), func);
            builder.CreateBr(loopBB);
            builder.SetInsertPoint(loopBB);

            auto loopCounter = builder.CreateAlignedLoad(loopCounterAlloc, 8u);

            auto conjunctAIndex = builder.CreateAdd(
                    builder.createConstMul(args[count], conjunctPosition),
                    loopCounter);
            auto conjunctAElement = builder.CreateInBoundsGEP(args[resultData], conjunctAIndex);
            auto conjunctBIndex = builder.CreateAdd(
                    builder.createConstMul(args[count], conjunctPosition + 1),
                    loopCounter);
            auto conjunctBElement = builder.CreateInBoundsGEP(args[resultData], conjunctBIndex);

            auto res = builder.CreateAnd(builder.CreateLoad(conjunctAElement), builder.CreateLoad(conjunctBElement));
            builder.CreateStore(res, conjunctAElement);

            // -> i += 1;
            auto nextVar = builder.CreateAdd(loopCounter, builder.getInt64(1u));
            builder.CreateAlignedStore(nextVar, loopCounterAlloc, 8u);

            // i < page->count
            auto endCond = builder.CreateICmp(ICmpInst::ICMP_ULT, nextVar, args[count]);

            // Create the loop
            auto afterBB = BasicBlock::Create(mCompilerContext, "endconj." + Twine(conjunctPosition), func);
            builder.CreateCondBr(endCond, loopBB, afterBB);
            builder.SetInsertPoint(afterBB);
        }

        // i = 0u;
        builder.CreateAlignedStore(builder.getInt64(0u), loopCounterAlloc, 8u);

        auto loopBB = BasicBlock::Create(mCompilerContext, "conj." + Twine(i), func);
        builder.CreateBr(loopBB);
        builder.SetInsertPoint(loopBB);

        auto loopCounter = builder.CreateAlignedLoad(loopCounterAlloc, 8u);

        Value* conjunctAIndex = loopCounter;
        if (i > 0) {
            conjunctAIndex = builder.CreateAdd(conjunctAIndex, builder.createConstMul(args[count], i));
        }
        auto conjunctAElement = builder.CreateInBoundsGEP(args[resultData], conjunctAIndex);

        auto conjunctBIndex = builder.CreateAdd(
                builder.createConstMul(args[count], state.conjunctOffset),
                loopCounter);
        auto conjunctBElement = builder.CreateInBoundsGEP(args[resultData], conjunctBIndex);

        auto res = builder.CreateAnd(builder.CreateLoad(conjunctAElement), builder.CreateLoad(conjunctBElement));
        builder.CreateStore(res, conjunctAElement);

        // -> i += 1;
        auto nextVar = builder.CreateAdd(loopCounter, builder.getInt64(1u));
        builder.CreateAlignedStore(nextVar, loopCounterAlloc, 8u);

        // i < page->count
        auto endCond = builder.CreateICmp(ICmpInst::ICMP_ULT, nextVar, args[count]);

        // Create the loop
        auto afterBB = BasicBlock::Create(mCompilerContext, "endconj." + Twine(i), func);
        builder.CreateCondBr(endCond, loopBB, afterBB);
        builder.SetInsertPoint(afterBB);
    }

    // Return
    builder.CreateRetVoid();
}

ColumnMapScanProcessor::ColumnMapScanProcessor(const ColumnMapContext& context, const Record& record,
        const std::vector<ScanQuery*>& queries, const PageList& pages, size_t pageIdx, size_t pageEndIdx,
        const LogIterator& logIter, const LogIterator& logEnd, ColumnMapScan::ColumnScanFun columnScanFun,
        ColumnMapScan::RowScanFun rowScanFun, uint32_t numConjuncts)
        : LLVMRowScanProcessorBase(record, queries, rowScanFun, numConjuncts),
          mContext(context),
          mColumnScanFun(columnScanFun),
          pages(pages),
          pageIdx(pageIdx),
          pageEndIdx(pageEndIdx),
          logIter(logIter),
          logEnd(logEnd) {
}

void ColumnMapScanProcessor::process() {
    for (auto i = pageIdx; i < pageEndIdx; ++i) {
        processMainPage(pages[i]);
    }
    for (auto insIter = logIter; insIter != logEnd; ++insIter) {
        if (!insIter->sealed()) {
            continue;
        }
        processInsertRecord(reinterpret_cast<const InsertLogEntry*>(insIter->data()));
    }
}

void ColumnMapScanProcessor::processMainPage(const ColumnMapMainPage* page) {
    mKeyData.reserve(page->count);
    mValidFromData.reserve(page->count);
    mValidToData.reserve(page->count);
    mResult.resize(mNumConjuncts * page->count, 0u);

    auto entries = page->entryData();
    typename std::remove_const<decltype(page->count)>::type i = 0;
    while (i < page->count) {
        auto key = entries[i].key;
        auto newest = entries[i].newest.load();
        auto validTo = std::numeric_limits<uint64_t>::max();
        if (newest != 0u) {
            if ((newest & crossbow::to_underlying(NewestPointerTag::INVALID)) != 0x0u) {
                // Set the valid-to version to 0 so the queries ignore the tuple
                for (; i < page->count && entries[i].key == key; ++i) {
                    mKeyData.emplace_back(0u);
                    mValidFromData.emplace_back(0u);
                    mValidToData.emplace_back(0u);
                }
                continue;
            }
            if (auto main = newestMainRecord(newest)) {
                LOG_ERROR("TODO Process relocated main entry");
                std::terminate();

                // Set the valid-to version to 0 so the queries ignore the tuple
                for (; i < page->count && entries[i].key == key; ++i) {
                    mKeyData.emplace_back(0u);
                    mValidFromData.emplace_back(0u);
                    mValidToData.emplace_back(0u);
                }
                continue;
            }

            auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(newest),
                    entries[i].version, validTo);

            // Skip elements with version above lowest version and set the valid-to version to 0 to exclude them from
            // the query processing
            for (; i < page->count && entries[i].key == key && entries[i].version >= lowestVersion; ++i) {
                mKeyData.emplace_back(0u);
                mValidFromData.emplace_back(0u);
                mValidToData.emplace_back(0u);
            }
        }

        // Set valid-to version for every element of the same key to the valid-from version of the previous
        for (; i < page->count && entries[i].key == key; ++i) {
            mKeyData.emplace_back(key);
            mValidFromData.emplace_back(entries[i].version);
            mValidToData.emplace_back(validTo);
            validTo = entries[i].version;
        }
    }
    LOG_ASSERT(mKeyData.size() == page->count, "Size of key array does not match the page size");
    LOG_ASSERT(mValidFromData.size() == page->count, "Size of valid-from array does not match the page size");
    LOG_ASSERT(mValidToData.size() == page->count, "Size of valid-to array does not match the page size");

    mColumnScanFun(&mKeyData.front(), &mValidFromData.front(), &mValidToData.front(), page->recordData(),
            page->heapData(), page->count, &mResult.front());

    auto sizeData = page->sizeData();
    auto result = &mResult.front();
    for (auto& con : mQueries) {
        for (typename std::remove_const<decltype(page->count)>::type i = 0; i < page->count; ++i) {
            if (result[i] == 0u) {
                continue;
            }
            auto length = sizeData[i];
            con.writeRecord(entries[i].key, length, entries[i].version, mValidToData[i],
                    [this, page, i, length] (char* dest) {
                mContext.materialize(page, i, dest, length);
            });
        }
        result += page->count;
    }

    mKeyData.clear();
    mValidFromData.clear();
    mValidToData.clear();
    mResult.clear();
}

void ColumnMapScanProcessor::processInsertRecord(const InsertLogEntry* ptr) {
    ConstInsertRecord record(ptr);
    if (!record.valid()) {
        return;
    }

    if (auto main = newestMainRecord(record.newest())) {
        LOG_ERROR("TODO Process relocated main entry");
        std::terminate();
    }

    auto validTo = std::numeric_limits<uint64_t>::max();
    if (record.newest() != 0u) {
        auto lowestVersion = processUpdateRecord(reinterpret_cast<const UpdateLogEntry*>(record.newest()),
                record.baseVersion(), validTo);

        if (ptr->version >= lowestVersion) {
            return;
        }
    }
    auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(ptr));
    processRowRecord(ptr->key, ptr->version, validTo, ptr->data(), entry->size() - sizeof(InsertLogEntry));
}

uint64_t ColumnMapScanProcessor::processUpdateRecord(const UpdateLogEntry* ptr, uint64_t baseVersion,
        uint64_t& validTo) {
    UpdateRecordIterator updateIter(ptr, baseVersion);
    for (; !updateIter.done(); updateIter.next()) {
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(updateIter.value()));

        // Check if the entry marks a deletion: Skip element
        if (entry->type() == crossbow::to_underlying(RecordType::DELETE)) {
            validTo = updateIter->version;
            continue;
        }

        processRowRecord(updateIter->key, updateIter->version, validTo, updateIter->data(),
                entry->size() - sizeof(UpdateLogEntry));
        validTo = updateIter->version;
    }
    return updateIter.lowestVersion();
}

} // namespace deltamain
} // namespace store
} // namespace tell
