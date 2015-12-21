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

#include <tellstore/Record.hpp>

#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/IRBuilder.h>

#include <cstdint>

namespace tell {
namespace store {

/**
 * @brief Helper class extending the standard LLVM IR Builder with some additional functionality
 */
class LLVMBuilder : public llvm::IRBuilder<> {
public:
    static llvm::Type* getVectorTy(llvm::Type* type, uint64_t vectorSize) {
        return vectorSize == 1 ? type : llvm::VectorType::get(type, vectorSize);
    }

    static llvm::Constant* getVector(uint64_t vectorSize, llvm::Constant* value) {
        return vectorSize == 1 ? value : llvm::ConstantVector::getSplat(vectorSize, value);
    }

    LLVMBuilder(llvm::LLVMContext& context)
            : llvm::IRBuilder<>(context) {
    }

    llvm::Type* getInt1VectorTy(uint64_t vectorSize) {
        return getVectorTy(getInt1Ty(), vectorSize);
    }

    llvm::PointerType* getInt1VectorPtrTy(uint64_t vectorSize, unsigned AddrSpace = 0) {
        return getInt1VectorTy(vectorSize)->getPointerTo(AddrSpace);
    }

    llvm::Type* getInt8VectorTy(uint64_t vectorSize) {
        return getVectorTy(getInt8Ty(), vectorSize);
    }

    llvm::PointerType* getInt8VectorPtrTy(uint64_t vectorSize, unsigned AddrSpace = 0) {
        return getInt8VectorTy(vectorSize)->getPointerTo(AddrSpace);
    }

    llvm::PointerType* getInt16PtrTy(unsigned AddrSpace = 0) {
        return llvm::Type::getInt16PtrTy(Context, AddrSpace);
    }

    llvm::PointerType* getInt32PtrTy(unsigned AddrSpace = 0) {
        return llvm::Type::getInt32PtrTy(Context, AddrSpace);
    }

    llvm::Type* getInt32VectorTy(uint64_t vectorSize) {
        return getVectorTy(getInt32Ty(), vectorSize);
    }

    llvm::PointerType* getInt64PtrTy(unsigned AddrSpace = 0) {
        return llvm::Type::getInt64PtrTy(Context, AddrSpace);
    }

    llvm::Type* getInt64VectorTy(uint64_t vectorSize) {
        return getVectorTy(getInt64Ty(), vectorSize);
    }

    llvm::PointerType* getInt64VectorPtrTy(uint64_t vectorSize, unsigned AddrSpace = 0) {
        return getInt64VectorTy(vectorSize)->getPointerTo(AddrSpace);
    }

    llvm::Constant* getInt64Vector(uint64_t vectorSize, uint64_t C) {
        return getVector(vectorSize, getInt64(C));
    }

    llvm::Type* getFloatTy() {
        return llvm::Type::getFloatTy(Context);
    }

    llvm::PointerType* getFloatPtrTy(unsigned AddrSpace = 0) {
        return llvm::Type::getFloatPtrTy(Context, AddrSpace);
    }

    llvm::Constant* getFloat(float C) {
        return llvm::ConstantFP::get(getFloatTy(), C);
    }

    llvm::Constant* getFloatVector(uint64_t vectorSize, float C) {
        return getVector(vectorSize, getFloat(C));
    }

    llvm::Type* getDoubleTy() {
        return llvm::Type::getDoubleTy(Context);
    }

    llvm::PointerType* getDoublePtrTy(unsigned AddrSpace = 0) {
        return llvm::Type::getDoublePtrTy(Context, AddrSpace);
    }

    llvm::Constant* getDouble(double C) {
        return llvm::ConstantFP::get(getDoubleTy(), C);
    }

    llvm::Constant* getDoubleVector(uint64_t vectorSize, double C) {
        return getVector(vectorSize, getDouble(C));
    }

    llvm::Type* getFieldTy(FieldType field);

    llvm::PointerType* getFieldPtrTy(FieldType field, unsigned AddrSpace = 0);

    llvm::Type* getFieldVectorTy(FieldType field, uint64_t vectorSize) {
        return getVectorTy(getFieldTy(field), vectorSize);
    }

    llvm::PointerType* getFieldVectorPtrTy(FieldType field, uint64_t vectorSize, unsigned AddrSpace = 0) {
        return getFieldVectorTy(field, vectorSize)->getPointerTo(AddrSpace);
    }

    llvm::CmpInst::Predicate getIntPredicate(PredicateType type);

    llvm::CmpInst::Predicate getFloatPredicate(PredicateType type);

    /**
     * @brief Create an optimized multiplication operation with a constant
     */
    llvm::Value* createConstMul(llvm::Value* lhs, uint64_t rhs);

    /**
     * @brief Create an optimized modulo operation with a constant
     */
    llvm::Value* createConstMod(llvm::Value* lhs, uint64_t rhs, uint64_t vectorSize = 0);

    /**
     * @brief Create an pointer alignment operation with a constant
     */
    llvm::Value* createPointerAlign(llvm::Value* value, uintptr_t alignment);
};

/**
 * @brief Helper class to build a LLVM function
 */
class FunctionBuilder : public LLVMBuilder {
public:
    /**
     * @brief Create a new function
     *
     * @param module Module the new function should be created in
     * @param target Properties of the target that should be associated with the function
     * @param returnType Return type of the function
     * @param params Type and name of all parameters of the function
     * @param name Name of the function
     */
    FunctionBuilder(llvm::Module& module, llvm::TargetMachine* target, llvm::Type* returnType,
            std::vector<std::pair<llvm::Type*, crossbow::string>> params, const llvm::Twine& name);

    llvm::Function* getFunction() {
        return mFunction;
    }

    llvm::Value* getParam(size_t idx) {
        return mParams[idx];
    }

    llvm::BasicBlock* createBasicBlock(const llvm::Twine& name = "") {
        return llvm::BasicBlock::Create(Context, name, mFunction);
    }

    /**
     * @brief Create a new loop
     *
     * @param start Start index of the loop
     * @param end End index of the loop
     * @param step Step count of every iteration
     * @param name Name of the loop blocks
     * @param fun Function with the signature void(llvm::Value*) producing the loop code for the supplied index
     * @return The end index of the loop
     */
    template <typename Fun>
    llvm::Value* createLoop(llvm::Value* start, llvm::Value* end, uint64_t step, const llvm::Twine& name, Fun fun);

    /**
     * @brief Creates a memcmp function
     *
     * The rhs string must be at least as long as the lhs string.
     *
     * @param cond Initial condition that must be true
     * @param lhsStart Pointer to string A
     * @param rhsStart Pointer to string B
     * @param rhsEnd End pointer to string B
     * @param name Name of the loop blocks
     * @return The result of the comparison
     */
    llvm::Value* createMemCmp(llvm::Value* cond, llvm::Value* lhsStart, llvm::Value* rhsStart, llvm::Value* rhsEnd,
            const llvm::Twine& name = "");

    /**
     * @brief Creates a memcmp function that matches on the postfix
     *
     * @param lhsStart Pointer to string A
     * @param lhsLength Length of string A
     * @param rhsString Pointer to global char array B
     * @param rhsLength Length of string B
     * @param name Name of the loop blocks
     * @return The result of the comparison
     */
    llvm::Value* createPostfixMemCmp(llvm::Value* lhsStart, llvm::Value* lhsLength, llvm::GlobalValue* rhsString,
            uint32_t rhsLength, const llvm::Twine& name = "");

protected:
    llvm::Function* mFunction;

    std::vector<llvm::Value*> mParams;

    llvm::TargetTransformInfo mTargetInfo;
};

template <typename Fun>
llvm::Value* FunctionBuilder::createLoop(llvm::Value* start, llvm::Value* end, uint64_t step, const llvm::Twine& name,
        Fun fun) {
    if (step != 1) {
        auto count = CreateSub(end, start);
        count = CreateAnd(count, getInt64(-step));
        end = CreateAdd(start, count);
    }

    auto previousBlock = GetInsertBlock();
    auto bodyBlock = createBasicBlock(name + ".body");
    auto endBlock = llvm::BasicBlock::Create(Context, name + ".end");
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, start, end), bodyBlock, endBlock);
    SetInsertPoint(bodyBlock);

    // -> auto i = start;
    auto i = CreatePHI(getInt64Ty(), 2);
    i->addIncoming(start, previousBlock);

    fun(i);

    // -> i += 1;
    auto next = CreateAdd(i, getInt64(step));
    i->addIncoming(next, GetInsertBlock());

    // -> i != endIdx
    CreateCondBr(CreateICmp(llvm::CmpInst::ICMP_NE, next, end), bodyBlock, endBlock);

    mFunction->getBasicBlockList().push_back(endBlock);
    SetInsertPoint(endBlock);
    return end;
}

} // namespace store
} // namespace tell
