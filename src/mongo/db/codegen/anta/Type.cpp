#include "mongo/db/codegen/anta/EnvCodeGenCtx.h"
#include "mongo/db/codegen/anta/Function.h"
#include "mongo/db/codegen/anta/Type.h"
#include "mongo/db/codegen/anta/Scope.h"

#include <llvm/IR/Module.h>

namespace anta
{
	bool Type::canCast(const Type*t) const
	{
		if (isInteger() && t->is_a<DoubleType>())
		{
			return true;
		}
		return false;
	}

	bool StringType::canCast(const Type * dest) const
	{
		if (linear() && !dest->linear() && dest->is_a<StringType>())
			return true;

		return PointerType::canCast(dest);
	}

	void StringType::generateUndef(EnvCodeGenCtx& ectx, llvm::Value *ptr) const
	{
		llvm::Type* type = getllvm(ectx.ctx_);
		ectx.builder_.CreateStore(llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(type)), ptr);
	}

	void StringType::generateCheckUndef(EnvCodeGenCtx& ectx, llvm::Value *value) const
	{
		auto condition = ectx.builder_.CreateICmpEQ(value, llvm::Constant::getNullValue(value->getType()));

		llvm::BasicBlock *FailNullBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "checkfail", ectx.function_);
		llvm::BasicBlock *MergeBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "checkpass");

		ectx.builder_.CreateCondBr(condition, FailNullBB, MergeBB);

		// Emit the check fail.
		ectx.builder_.SetInsertPoint(FailNullBB);
		auto trap_func = llvm::Intrinsic::getDeclaration(
			ectx.ctx_.module_.get(),
			llvm::Intrinsic::trap);

		ectx.builder_.CreateCall(trap_func);
		ectx.builder_.CreateUnreachable();

		// Emit merge block.
		ectx.function_->getBasicBlockList().push_back(MergeBB);
		ectx.builder_.SetInsertPoint(MergeBB);
	}

	void StringType::generateKill(EnvCodeGenCtx& ectx, llvm::Value *value) const
	{
		auto condition = ectx.builder_.CreateICmpNE(value, llvm::Constant::getNullValue(value->getType()));

		llvm::BasicBlock *KillBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "kill", ectx.function_);
		llvm::BasicBlock *MergeBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "killkmerge");

		ectx.builder_.CreateCondBr(condition, KillBB, MergeBB);

		// Emit the kill.
		ectx.builder_.SetInsertPoint(KillBB);

		ectx.CreateCall(scope()->getFunction("killString"), {value});
		ectx.CreateBr(MergeBB);

		// Emit merge block.
		ectx.function_->getBasicBlockList().push_back(MergeBB);
		ectx.builder_.SetInsertPoint(MergeBB);
	}

	bool PointerType::canCast(const Type* dest) const
	{
		if (!linear() && !dest->linear() && dest->is_a<PointerType>())
			return true;

		return Type::canCast(dest);
	}

	FunctionType::FunctionType(Function* fn) : Type(fn->scope(), fn->fullName()), fn_(fn)
	{
	}

	llvm::Type* FunctionType::getllvm(CodeGenContext& ctx) const
	{
		llvm::Function* f = ctx.module_->getFunction(name());
		
		return f->getFunctionType();
	}

	void StructType::generateUndef(EnvCodeGenCtx & ectx, llvm::Value *ptr) const
	{
		for (unsigned i = 0; i < types_.size(); ++i)
		{
			auto& t = types_[i];
			if (t->linear())
			{
				llvm::Value* idx[2];
				idx[0] = llvm::ConstantInt::get(llvm::Type::getInt32Ty(ectx.ctx_.context_), 0, true);
				idx[1] = llvm::ConstantInt::get(llvm::Type::getInt32Ty(ectx.ctx_.context_), i, true);

				t->generateUndef(ectx, ectx.builder_.CreateInBoundsGEP(nullptr, ptr, idx));
			}
		}
	}

	void StructType::generateKill(EnvCodeGenCtx & ectx, llvm::Value *value) const
	{
		for (unsigned i = 0; i < types_.size(); ++i)
		{
			auto& t = types_[i];
			if (t->linear())
			{
				unsigned idx[1];
				idx[0] = i;

				t->generateKill(ectx, ectx.builder_.CreateExtractValue(value, idx));
			}
		}
	}

	bool BSONVariant::canCast(const Type * dest) const
	{
		if (linear() && !dest->linear() && dest->is_a<BSONVariant>())
			return true;

		return ArrayType::canCast(dest);
	}

	void BSONVariant::generateUndef(EnvCodeGenCtx & ectx, llvm::Value *ptr) const
	{
		auto rawPtr = ectx.builder_.CreateBitCast(ptr, llvm::Type::getInt8PtrTy(ectx.ctx_.context_));

		rawPtr = ectx.builder_.CreateConstGEP1_32(nullptr, rawPtr, kTagOffset);

		//Store 0 to the tag byte
		ectx.builder_.CreateStore(llvm::Constant::getNullValue(llvm::Type::getInt8Ty(ectx.ctx_.context_)), rawPtr);
	}

	void BSONVariant::generateCheckUndef(EnvCodeGenCtx & ectx, llvm::Value *value) const
	{
		auto shifted = ectx.builder_.CreateLShr(value, (uint64_t)(8 * kTagOffset));
		auto tagByte = ectx.builder_.CreateTrunc(shifted, llvm::Type::getInt8Ty(ectx.ctx_.context_));
		auto condition = ectx.builder_.CreateICmpEQ(tagByte, llvm::Constant::getNullValue(tagByte->getType()));

		llvm::BasicBlock *FailNullBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "checkfail", ectx.function_);
		llvm::BasicBlock *MergeBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "checkpass");

		ectx.builder_.CreateCondBr(condition, FailNullBB, MergeBB);

		// Emit the check fail.
		ectx.builder_.SetInsertPoint(FailNullBB);
		auto trap_func = llvm::Intrinsic::getDeclaration(
			ectx.ctx_.module_.get(),
			llvm::Intrinsic::trap);

		ectx.builder_.CreateCall(trap_func);
		ectx.builder_.CreateUnreachable();

		// Emit merge block.
		ectx.function_->getBasicBlockList().push_back(MergeBB);
		ectx.builder_.SetInsertPoint(MergeBB);
	}

	void BSONVariant::generateKill(EnvCodeGenCtx & ectx, llvm::Value *value) const
	{
		auto shifted = ectx.builder_.CreateLShr(value, (uint64_t)(8 * kTagOffset));
		auto tagByte = ectx.builder_.CreateTrunc(shifted, llvm::Type::getInt8Ty(ectx.ctx_.context_));
		auto afterAnd = ectx.builder_.CreateAnd(tagByte, 0x80);
		auto isShallow = ectx.builder_.CreateICmpEQ(afterAnd, llvm::Constant::getNullValue(afterAnd->getType()));

		llvm::BasicBlock *FailNullBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "deepkill", ectx.function_);
		llvm::BasicBlock *MergeBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "shallowkill");

		ectx.builder_.CreateCondBr(isShallow, MergeBB, FailNullBB);

		// Emit the deep kill.
		ectx.builder_.SetInsertPoint(FailNullBB);

		auto shiftMem = ectx.builder_.CreateLShr(value, (uint64_t)(8 * kMemoryPtrOffset));
		auto ptrMem = ectx.builder_.CreateIntToPtr(shiftMem, llvm::Type::getInt8PtrTy(ectx.ctx_.context_));

		auto shiftAlloc = ectx.builder_.CreateLShr(value, (uint64_t)(8 * kAllocatorPtrOffset));
		auto ptrAlloc = ectx.builder_.CreateIntToPtr(shiftAlloc, llvm::Type::getInt8PtrTy(ectx.ctx_.context_));

		ectx.CreateCall(scope()->getFunction("releaseMemory"), { ptrAlloc, ptrMem });

		ectx.CreateBr(MergeBB);

		// Emit merge block.
		ectx.function_->getBasicBlockList().push_back(MergeBB);
		ectx.builder_.SetInsertPoint(MergeBB);
	}

}