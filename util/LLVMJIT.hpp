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
 *
 * --------------------------------------------------------------------
 *
 * This file was copied and slightly adapted from the KaleidoscopeJIT.h
 * unter the following licencse:
 *
 * ===----- KaleidoscopeJIT.h - A simple JIT for Kaleidoscope ----*- C++ -*-===
 *
 *                     The LLVM Compiler Infrastructure
 *
 * This file is distributed under the University of Illinois Open Source
 * License. See LICENSE.TXT for details.
 *
 */

#pragma once

#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/Mangler.h>

#include <crossbow/singleton.hpp>

#include <memory>

namespace llvm {
class Module;
class TargetMachine;
} // namespace llvm

namespace tell {
namespace store {

/**
 * @brief Singleton for initializing and configuring the LLVM infrastructure
 *
 * Initializes the native target and sets up the TargetMachine with the features available on the host the application
 * is running.
 */
class LLVMCompilerT {
public:
    LLVMCompilerT();

    ~LLVMCompilerT();

    llvm::TargetMachine* targetMachine() {
        return mTargetMachine.get();
    }

private:
    std::unique_ptr<llvm::TargetMachine> mTargetMachine;
};

using LLVMCompiler = crossbow::singleton<LLVMCompilerT>;

extern LLVMCompiler llvmCompiler;

/**
 * @brief JIT based on LLVM
 */
class LLVMJIT {
public:
    using ObjectLayer = llvm::orc::ObjectLinkingLayer<>;
    using CompileLayer = llvm::orc::IRCompileLayer<ObjectLayer>;
    using ModuleHandle = CompileLayer::ModuleSetHandleT;

    LLVMJIT();

    const llvm::TargetMachine* getTargetMachine() {
        return mTargetMachine;
    }

    /**
     * @brief Compile the given module through the JIT
     *
     * The compiled functions can then be retrieved through the LLVMJIT::findSymbol function. Afterwards the module can
     * be destroyed.
     */
    ModuleHandle addModule(llvm::Module* module);

    void removeModule(ModuleHandle handle) {
        mCompileLayer.removeModuleSet(handle);
    }

    llvm::orc::JITSymbol findSymbol(const std::string& name) {
        return mCompileLayer.findSymbol(mangle(name), true);
    }

private:
    std::string mangle(const std::string& name) {
        std::string mangledName;
        {
            llvm::raw_string_ostream mangledNameStream(mangledName);
            llvm::Mangler::getNameWithPrefix(mangledNameStream, name, mDataLayout);
        }
        return mangledName;
    }

    llvm::TargetMachine* mTargetMachine;
    llvm::DataLayout mDataLayout;
    ObjectLayer mObjectLayer;
    CompileLayer mCompileLayer;
};

} // namespace store
} // namespace tell
