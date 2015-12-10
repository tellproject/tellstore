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

#include <llvm/IR/Module.h>
#include <llvm/Target/TargetMachine.h>

namespace tell {
namespace store {

llvm::Value* LLVMBuilder::createConstMul(llvm::Value* lhs, uint64_t rhs) {
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

llvm::Value* LLVMBuilder::createConstMod(llvm::Value* lhs, uint64_t rhs, uint64_t vectorSize /* = 0 */) {
    if (rhs == 0u) {
        throw std::invalid_argument("Modulo by 0");
    }
    if (rhs == 1u) {
        return getInt64Vector(vectorSize, 0);
    }
    auto mask = rhs - 1;
    if (rhs & mask) {
        return CreateURem(lhs, getInt64Vector(vectorSize, rhs));
    }

    return CreateAnd(lhs, getInt64Vector(vectorSize, mask));
}

llvm::Value* LLVMBuilder::createPointerAlign(llvm::Value* value, uintptr_t alignment) {
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

llvm::Type* LLVMBuilder::getFieldTy(FieldType field) {
    switch (field) {
    case FieldType::SMALLINT:
        return getInt16Ty();

    case FieldType::INT:
        return getInt32Ty();

    case FieldType::BIGINT:
        return getInt64Ty();

    case FieldType::FLOAT:
        return getFloatTy();

    case FieldType::DOUBLE:
        return getDoubleTy();

    default:
        LOG_ASSERT(false, "Only fixed size fields are allowed");
        return nullptr;
    }
}

llvm::PointerType* LLVMBuilder::getFieldPtrTy(FieldType field, unsigned AddrSpace /* = 0 */) {
    switch (field) {
    case FieldType::SMALLINT:
        return getInt16PtrTy(AddrSpace);

    case FieldType::INT:
        return getInt32PtrTy(AddrSpace);

    case FieldType::BIGINT:
        return getInt64PtrTy(AddrSpace);

    case FieldType::FLOAT:
        return getFloatPtrTy(AddrSpace);

    case FieldType::DOUBLE:
        return getDoublePtrTy(AddrSpace);

    case FieldType::TEXT:
    case FieldType::BLOB:
        return getInt32PtrTy(AddrSpace);

    default:
        LOG_ASSERT(false, "Only fixed size fields are allowed");
        return nullptr;
    }
}

llvm::CmpInst::Predicate LLVMBuilder::getIntPredicate(PredicateType type) {
    using namespace llvm;

    switch (type) {
    case PredicateType::EQUAL:
        return CmpInst::ICMP_EQ;

    case PredicateType::NOT_EQUAL:
        return CmpInst::ICMP_NE;

    case PredicateType::LESS:
        return CmpInst::ICMP_SLT;

    case PredicateType::LESS_EQUAL:
        return CmpInst::ICMP_SLE;

    case PredicateType::GREATER:
        return CmpInst::ICMP_SGT;

    case PredicateType::GREATER_EQUAL:
        return CmpInst::ICMP_SGE;

    default: {
        LOG_ASSERT(false, "Unknown or invalid predicate");
        return CmpInst::BAD_ICMP_PREDICATE;
    }
    }
}

llvm::CmpInst::Predicate LLVMBuilder::getFloatPredicate(PredicateType type) {
    using namespace llvm;

    switch (type) {
    case PredicateType::EQUAL:
        return CmpInst::FCMP_OEQ;

    case PredicateType::NOT_EQUAL:
        return CmpInst::FCMP_ONE;

    case PredicateType::LESS:
        return CmpInst::FCMP_OLT;

    case PredicateType::LESS_EQUAL:
        return CmpInst::FCMP_OLE;

    case PredicateType::GREATER:
        return CmpInst::FCMP_OGT;

    case PredicateType::GREATER_EQUAL:
        return CmpInst::FCMP_OGE;

    default: {
        LOG_ASSERT(false, "Unknown or invalid predicate");
        return CmpInst::BAD_FCMP_PREDICATE;
    }
    }
}

FunctionBuilder::FunctionBuilder(llvm::Module& module, llvm::TargetMachine* target, llvm::Type* returnType,
        std::vector<std::pair<llvm::Type*, crossbow::string>> params, const llvm::Twine& name)
        : LLVMBuilder(module.getContext()),
          mTargetInfo(module.getDataLayout()) {
    std::vector<llvm::Type*> paramTypes;
    paramTypes.reserve(params.size());
    for (auto& p : params) {
        paramTypes.emplace_back(p.first);
    }
    auto functionType = llvm::FunctionType::get(returnType, paramTypes, false);
    mFunction = llvm::Function::Create(functionType, llvm::Function::ExternalLinkage, name, &module);
    mFunction->addFnAttr(llvm::Attribute::NoUnwind);
    mFunction->addFnAttr("target-cpu", target->getTargetCPU());
    mFunction->addFnAttr("target-features", target->getTargetFeatureString());

    mTargetInfo = target->getTargetIRAnalysis().run(*mFunction);

    mParams.reserve(params.size());
    decltype(params.size()) idx = 0;
    for (auto iter = mFunction->arg_begin(); idx != params.size(); ++iter, ++idx) {
        iter->setName(params[idx].second.c_str());
        mParams.emplace_back(iter.operator ->());
    }

    auto entryBlock = llvm::BasicBlock::Create(Context, "entry", mFunction);
    SetInsertPoint(entryBlock);
}

} // namespace store
} // namespace tell
