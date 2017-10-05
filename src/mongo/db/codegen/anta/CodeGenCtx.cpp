#include "mongo/db/codegen/anta/CodeGenCtx.h"

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/Function.h>
#include <llvm/Support/DynamicLibrary.h>

namespace anta
{
	CodeGenContext::CodeGenContext(const char *moduleName)
	{
		module_ = std::make_shared<llvm::Module>(moduleName, context_);
	}

	void CodeGenContext::addNativeFunction(llvm::Function* f, void * fn)
	{
		std::string name = f->getName();
		llvm::sys::DynamicLibrary::AddSymbol(name, fn);
	}
}