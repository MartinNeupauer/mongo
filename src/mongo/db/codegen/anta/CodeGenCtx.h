#pragma once

#include <llvm/IR/LLVMContext.h>

#include <memory>

namespace llvm
{
    class Module;
	class Function;
}

namespace anta
{
    struct CodeGenContext
    {
        llvm::LLVMContext                  context_;
        std::shared_ptr<llvm::Module>      module_;

		CodeGenContext(const char *moduleName);

		void addNativeFunction(llvm::Function* f, void* fn);
    };
}