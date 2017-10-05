#pragma once

#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/Orc/IRTransformLayer.h"
#include <llvm/IR/DataLayout.h>
#include "llvm/Target/TargetMachine.h"

#include <memory>

namespace machine
{
	class MemoryManager;

	class Jitter
	{
		using TransformFunctionType = std::function<std::shared_ptr<llvm::Module>(std::shared_ptr<llvm::Module>)>;
		using CompileFunctionType = std::function<llvm::object::OwningBinary<llvm::object::ObjectFile>(llvm::Module& module)>;

		std::shared_ptr<MemoryManager> _memoryManager;
		std::unique_ptr<llvm::TargetMachine> _targetMachine;
		const llvm::DataLayout _dataLayout;
		llvm::orc::RTDyldObjectLinkingLayer _objectLinkingLayer;
		llvm::orc::IRCompileLayer<decltype(_objectLinkingLayer), CompileFunctionType> _compileLayer;
		llvm::orc::IRTransformLayer<decltype(_compileLayer), TransformFunctionType> _optimizeLayer;
		auto& topLayer() { return _optimizeLayer; }

		llvm::object::OwningBinary<llvm::object::ObjectFile> compile(llvm::Module& module);
		std::shared_ptr<llvm::Module> optimize(std::shared_ptr<llvm::Module> module);

	public:
		Jitter();

		void jit(std::shared_ptr<llvm::Module> module);

		void* findSymbol(const char* name);

		template <typename T>
		T find(const char* name)
		{
			return reinterpret_cast<T>(findSymbol(name));
		}
	};
}