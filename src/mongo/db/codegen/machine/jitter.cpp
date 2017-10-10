#include "mongo/platform/basic.h"

#include "mongo/db/codegen/machine/jitter.h"

#include <llvm/ExecutionEngine/JITSymbol.h>

#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/BasicAliasAnalysis.h>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/ObjectMemoryBuffer.h>
#include <llvm/ExecutionEngine/Orc/LambdaResolver.h>
#include <llvm/ExecutionEngine/RuntimeDyld.h>
#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Process.h>
#include <llvm/Support/Memory.h>

#include <unordered_map>
#include <initializer_list>

namespace machine
{
	static constexpr uintptr_t alignUp(uintptr_t x, unsigned a) { return (x + (a - 1ull)) & ~(a - 1ull); }

	// Custom memory manager for LLVM
	class MemoryManager final : public llvm::RuntimeDyld::MemoryManager
	{
		struct Regions
		{
			std::vector<uintptr_t> _offsets;
			std::vector<uintptr_t> _allocs;
			void reserve(std::initializer_list<uintptr_t> sizes, unsigned alignment)
			{
				_offsets.clear();
				_allocs.clear();

				_offsets.push_back(0);
				_offsets.insert(_offsets.end(), sizes.begin(), sizes.end());

				for (unsigned i = 1; i < _offsets.size(); ++i)
				{
					_offsets[i] = _offsets[i - 1] + alignUp(_offsets[i], alignment);
				}
				_allocs.insert(_allocs.begin(), _offsets.begin(), _offsets.end() - 1);
			}
			uint8_t* allocate(uint8_t* base, unsigned id, uintptr_t size, unsigned alignment)
			{
				_allocs[id] = alignUp(_allocs[id], alignment);
				if (_allocs[id] + size > _offsets[id+1])
					return nullptr;
				auto ret = base + _allocs[id];
				_allocs[id] += size;
				return ret;
			}
			std::error_code protect(uint8_t* base, unsigned id, unsigned flags)
			{
				llvm::sys::MemoryBlock block{ base + _offsets[id], _offsets[id+1] - _offsets[id] };
				auto errorCode = llvm::sys::Memory::protectMappedMemory(block, flags);
				if (!errorCode && (flags & llvm::sys::Memory::MF_EXEC))
					llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.size());

				return errorCode;
			}
			uintptr_t size() const { return _offsets.back(); }
		};

		enum RegionTypes : unsigned
		{
			rgCode,
			rgROData,
			rgRWData
		};

		llvm::SectionMemoryManager _memmgr;
		llvm::sys::MemoryBlock     _block;
		bool                       _ehRegistered;

		uint8_t* _base;
		Regions  _regions;

		std::unordered_map<std::string, uint64_t> _distance;

		unsigned _pageSize;
#ifdef _WIN32
		std::vector<RUNTIME_FUNCTION> _runtimeTable;

		void buildRuntimeTable(const llvm::object::ObjectFile& obj);
#endif
	public:
		MemoryManager()
			: llvm::RuntimeDyld::MemoryManager(),
			_ehRegistered(false),
			_base(nullptr),
			_pageSize(llvm::sys::Process::getPageSize())
		{
		}

		~MemoryManager() override
		{
			deregisterEHFrames();
		}

		//
		// LLVM API we have to implement to fulfill the contract
		//
		void registerEHFrames(uint8_t *Addr, uint64_t LoadAddr, size_t Size) override
		{
#ifdef _WIN32
			if (!RtlAddFunctionTable(_runtimeTable.data(), _runtimeTable.size(), reinterpret_cast<uint64_t>(_base)))
				throw std::runtime_error("RtlAddFunctionTable failed.");
#endif
			_memmgr.registerEHFrames(Addr, LoadAddr, Size);
			_ehRegistered = true;
		}

		void deregisterEHFrames() override
		{
			if (_ehRegistered)
			{
#ifdef _WIN32
				if (!RtlDeleteFunctionTable(_runtimeTable.data()))
					throw std::runtime_error("RtlDeleteFunctionTable failed.");
#endif
				_memmgr.deregisterEHFrames();
				_ehRegistered = false;
			}
		}

		uint8_t *allocateCodeSection(uintptr_t Size, unsigned Alignment, unsigned SectionID, llvm::StringRef SectionName) override
		{
			auto pointer = _regions.allocate(_base, rgCode, Size, Alignment);
			if (pointer)
				_distance.insert({ SectionName, pointer - _base });

			return pointer;
		}

		uint8_t *allocateDataSection(uintptr_t Size, unsigned Alignment, unsigned SectionID, llvm::StringRef SectionName, bool isReadOnly) override
		{
			auto pointer = _regions.allocate(_base, isReadOnly ? rgROData : rgRWData, Size, Alignment);
			if (pointer)
				_distance.insert({ SectionName, pointer - _base });

			return pointer;
		}

		bool finalizeMemory(std::string* = nullptr) override
		{
			std::error_code errorCode;

			if ((errorCode = _regions.protect(_base, rgCode, llvm::sys::Memory::MF_READ | llvm::sys::Memory::MF_EXEC)))
				return true;

			if ((errorCode = _regions.protect(_base, rgROData, llvm::sys::Memory::MF_READ)))
				return true;

			if ((errorCode = _regions.protect(_base, rgRWData, llvm::sys::Memory::MF_READ | llvm::sys::Memory::MF_WRITE)))
				return true;

			return false;
		}

		void notifyObjectLoaded(llvm::RuntimeDyld&, const llvm::object::ObjectFile& obj) override
		{
#ifdef _WIN32
			buildRuntimeTable(obj);
#endif
		}

		bool needsToReserveAllocationSpace() override { return true; }

		void reserveAllocationSpace(uintptr_t CodeSize, uint32_t CodeAlign, uintptr_t RODataSize, uint32_t RODataAlign, uintptr_t RWDataSize, uint32_t RWDataAlign) override
		{
			std::error_code errorCode;

			_regions.reserve({ CodeSize, RODataSize, RWDataSize },_pageSize);

			_block = llvm::sys::Memory::allocateMappedMemory(_regions.size(), nullptr, llvm::sys::Memory::MF_READ | llvm::sys::Memory::MF_WRITE, errorCode);

			if (errorCode)
				throw std::runtime_error(std::string{ "Memory::allocateMappedMemory: " } +errorCode.message());

			_base = reinterpret_cast<uint8_t*>(_block.base());
		}
	};

#ifdef _WIN32
	void MemoryManager::buildRuntimeTable(const llvm::object::ObjectFile& obj)
	{
		// The famous LLVM bug https://llvm.org/bugs/show_bug.cgi?id=24233

		llvm::StringRef name;
		for (auto section : obj.sections())
		{
			section.getName(name);

			if (name == ".pdata")
			{
				// LLVM damages the .pdata section so we make a copy here
				auto size = section.getSize();
				if (size % sizeof(RUNTIME_FUNCTION))
					throw std::runtime_error("The .pdata size must be multiple of RUNTIME_FUNCTION");

				_runtimeTable.resize(size / sizeof(RUNTIME_FUNCTION));
				
				auto pdata = _base + _distance[name];
				auto table = reinterpret_cast<uint8_t*>(_runtimeTable.data());

				std::copy(pdata, pdata + size, table);

				for (auto relocation : section.relocations())
				{
					if (relocation.getType() != IMAGE_REL_AMD64_ADDR32NB)
						throw std::runtime_error("Only IMAGE_REL_AMD64_ADDR32NB is supported in the .pdata section.");

					auto symbol = relocation.getSymbol();
						
					auto section = llvm::cantFail(symbol->getSection());
					section->getName(name);

					auto value = symbol->getValue() + _distance[name];
					assert(value <= 0xFFFFFFFFull);

					auto patchPointer = reinterpret_cast<uint32_t*>(table + relocation.getOffset());
					*patchPointer += static_cast<uint32_t>(value);
				}
				
			}
		}
	}
#endif

	static llvm::TargetMachine* constructTM()
	{
		llvm::EngineBuilder builder;
		
		builder.setOptLevel(llvm::CodeGenOpt::Aggressive);
#ifdef _WIN32
		builder.setCodeModel(llvm::CodeModel::Small);
#endif
/*
		builder.setOptLevel(llvm::CodeGenOpt::None);
		llvm::TargetOptions options;
		options.EnableFastISel = true;
		builder.setTargetOptions(options);
*/
		return builder.selectTarget();
	}

	Jitter::Jitter()
		: _memoryManager(std::make_shared<MemoryManager>())
		, _targetMachine(constructTM())
		, _dataLayout(_targetMachine->createDataLayout())
		, _objectLinkingLayer([this]() {return _memoryManager; })
		, _compileLayer(_objectLinkingLayer, [this](llvm::Module& module) { return compile(module); })
		, _optimizeLayer(_compileLayer, [this](std::shared_ptr<llvm::Module> module) { return optimize(module); })
	{
		llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
	}

	llvm::object::OwningBinary<llvm::object::ObjectFile> Jitter::compile(llvm::Module& module)
	{
		llvm::SmallVector<char, 0> ObjBufferSV;
		llvm::raw_svector_ostream ObjStream(ObjBufferSV);
		llvm::legacy::PassManager PM;
		llvm::MCContext *Ctx;
		if (_targetMachine->addPassesToEmitMC(PM, Ctx, ObjStream))
			throw std::logic_error("Target does not support MC emission.");

		PM.run(module);

		std::unique_ptr<llvm::MemoryBuffer> objBuffer(new llvm::ObjectMemoryBuffer(std::move(ObjBufferSV)));
		auto obj = llvm::object::ObjectFile::createObjectFile(objBuffer->getMemBufferRef());
		if (!obj) {
			return llvm::object::OwningBinary<llvm::object::ObjectFile>(nullptr, nullptr);
		}
		return llvm::object::OwningBinary<llvm::object::ObjectFile>(move(*obj), move(objBuffer));
	}

	std::shared_ptr<llvm::Module> Jitter::optimize(std::shared_ptr<llvm::Module> module)
	{
		auto fpm = std::make_unique<llvm::legacy::FunctionPassManager>(module.get());

		// Set up the optimizer pipeline.  Start with registering info about how the
		// target lays out data structures.
		module->setDataLayout(_dataLayout);
		// Provide basic AliasAnalysis support for GVN.
		fpm->add(llvm::createBasicAAWrapperPass());
		// Aggregates to scalars
		//fpm->add(llvm::createSROAPass());
		// Promote allocas to registers.
		fpm->add(llvm::createPromoteMemoryToRegisterPass());
		// Do simple "peephole" optimizations and bit-twiddling optzns.
		fpm->add(llvm::createInstructionCombiningPass());
		// Reassociate expressions.
		fpm->add(llvm::createReassociatePass());
		// Eliminate Common SubExpressions.
		fpm->add(llvm::createNewGVNPass());
		// Simplify the control flow graph (deleting unreachable blocks, etc).
		fpm->add(llvm::createCFGSimplificationPass());
		// Remove dead stores
		fpm->add(llvm::createDeadStoreEliminationPass());
		fpm->add(llvm::createDeadCodeEliminationPass());
		
		fpm->doInitialization();

		for (auto &function : *module)
			fpm->run(function);

		fpm->doFinalization();

		return module;
	}

	void Jitter::jit(std::shared_ptr<llvm::Module> module)
	{
		auto resolver = llvm::orc::createLambdaResolver(
			[&](const std::string& name) {
			if (auto sym = topLayer().findSymbol(name, false)) {
				return llvm::JITSymbol(cantFail(sym.getAddress()), sym.getFlags());
			}
			if (auto addr = llvm::RTDyldMemoryManager::getSymbolAddressInProcess(name)) {
				return llvm::JITSymbol(addr, llvm::JITSymbolFlags::Exported);
			}
			return llvm::JITSymbol(nullptr);
		},
			// Dylib lookup functor
			[&](const std::string& name) {
			if (auto addr = llvm::RTDyldMemoryManager::getSymbolAddressInProcess(name)) {
				return llvm::JITSymbol(addr, llvm::JITSymbolFlags::Exported);
			}
			return llvm::JITSymbol(nullptr);
		});

		auto m = cantFail(topLayer().addModule(module, std::move(resolver)));
		/*if (auto err = m.takeError())
		{
			return (void)err;
		}
		*/
		cantFail(topLayer().emitAndFinalize(m));
		/*if (auto err = topLayer().emitAndFinalize(*m))
		{
			return (void)err;
		}
		*/
	}

	void * Jitter::findSymbol(const char * name)
	{
		auto sym = topLayer().findSymbol(name, false);
		if (sym)
		{
			return (void*)cantFail(sym.getAddress());
		}
		return nullptr;
	}
}