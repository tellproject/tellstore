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

#include "LLVMJIT.hpp"

#include <llvm/ADT/STLExtras.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/LambdaResolver.h>
#include <llvm/ExecutionEngine/RuntimeDyld.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/CodeGen.h>
#include <llvm/Support/CommandLine.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Target/TargetOptions.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace tell {
namespace store {

LLVMCompiler llvmCompiler;

LLVMCompilerT::LLVMCompilerT()
        : mTarget(nullptr) {
    std::array<const char*, 2> args = {{
        "tellstore",
        "--disable-lsr"
    }};
    llvm::cl::ParseCommandLineOptions(args.size(), args.data());

    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();

    // Get the architecture name of the host
    mProcessTriple = llvm::sys::getProcessTriple();
    mHostCPUName = llvm::sys::getHostCPUName();

    // Setup all CPU features available on the host
    llvm::SubtargetFeatures features;
    llvm::StringMap<bool> hostFeatures;
    if (llvm::sys::getHostCPUFeatures(hostFeatures)) {
        for (auto& feature : hostFeatures) {
            features.AddFeature(feature.first(), feature.second);
        }
    }
    mFeatures = features.getString();

    // Lookup the LLVM target for the host
    std::string error;
    mTarget = llvm::TargetRegistry::lookupTarget(mProcessTriple, error);
    if (!mTarget) {
        throw std::runtime_error(error);
    }

    // Load the standard library
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

std::unique_ptr<llvm::TargetMachine> LLVMCompilerT::createTargetMachine() {
    return std::unique_ptr<llvm::TargetMachine>(mTarget->createTargetMachine(mProcessTriple, mHostCPUName, mFeatures,
            mOptions, llvm::Reloc::Default, llvm::CodeModel::JITDefault, llvm::CodeGenOpt::Aggressive));
}

LLVMJIT::LLVMJIT()
        : mTargetMachine(llvmCompiler->createTargetMachine()),
          mDataLayout(mTargetMachine->createDataLayout()),
          mCompileLayer(mObjectLayer, llvm::orc::SimpleCompiler(*mTargetMachine)) {
}

LLVMJIT::~LLVMJIT() = default;

LLVMJIT::ModuleHandle LLVMJIT::addModule(llvm::Module* module) {
    auto resolver = llvm::orc::createLambdaResolver([this] (const std::string& name) {
        if (auto sym = mCompileLayer.findSymbol(name, true)) {
            return llvm::RuntimeDyld::SymbolInfo(sym.getAddress(), sym.getFlags());
        }
        if (auto symAddr = llvm::RTDyldMemoryManager::getSymbolAddressInProcess(name)) {
            return llvm::RuntimeDyld::SymbolInfo(symAddr, llvm::JITSymbolFlags::Exported);
        }
        return llvm::RuntimeDyld::SymbolInfo(nullptr);
    }, [] (const std::string& /* name */) {
        return nullptr;
    });
    auto handle = mCompileLayer.addModuleSet(std::vector<llvm::Module*>{{module}},
            llvm::make_unique<llvm::SectionMemoryManager>(), std::move(resolver));
    mObjectLayer.emitAndFinalize(handle);
    return handle;
}

} // namespace store
} // namespace tell
