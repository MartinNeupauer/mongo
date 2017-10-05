#include "mongo/db/codegen/anta/Analysis.h"
#include "mongo/db/codegen/anta/Function.h"
#include "mongo/db/codegen/anta/EnvCodeGenCtx.h"
#include "mongo/db/codegen/anta/Expr.h"
#include "mongo/db/codegen/anta/DebugPrint.h"
#include "mongo/db/codegen/anta/Scope.h"


#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/CFG.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <unordered_map>
#include <iostream>

namespace anta
{
	// hack
	llvm::Value* EnvCodeGenCtx::CreateCall(const Function* function, llvm::ArrayRef<llvm::Value *> args)
	{
		llvm::Function* f = ctx_.module_->getFunction(function->isIntrinsic() ? function->name() : function->fullName());
		if (!f)
			throw std::logic_error("cannot find a function " + function->fullName());

		llvm::CallInst* call;
		if (function->getGlobalVar())
		{
			auto ptr = builder_.CreateLoad(function->getGlobalVar());
			call = builder_.CreateCall(ptr, args);
		}
		else
		{
			call = builder_.CreateCall(f, args);
		}

		if (function->retType() && function->retType()->linear() && function->retType()->is_a<PointerType>())
		{
			call->addAttribute(0, llvm::Attribute::NonNull);
		}

		return call;
	}

	static std::unordered_map<std::string, llvm::Intrinsic::ID> intrinsics = 
	{ 
		{"llvm.sadd.with.overflow.i32", llvm::Intrinsic::sadd_with_overflow},
		{"llvm.trap", llvm::Intrinsic::trap}
	};

	Function::Function(Scope* scope, const Type* type, Scope* paramScope, const std::string& name, void* builtin)
		: scope_(scope)
		, returnType_(type)
		, name_(name)
		, paramScope_(paramScope)
		, builtin_(builtin)
		, llvmfn_(nullptr)
		, globalVar_(nullptr)
		, intrinsicId_(0)
		, const_(false)
		, inline_(false)
		, body_(nullptr)
		{
		auto intrinsic = intrinsics.find(name);
		if (intrinsic != intrinsics.end())
		{
			intrinsicId_ = intrinsic->second;
		}
		functionType_ = scope->addType(fullName(), std::make_unique<FunctionType>(this));
    }

	void Function::setAndCheck(intrusive_ptr<Statement> body)
	{
		if (body_)
			throw std::logic_error("body already defined");
		
		body_ = body;

		if (body_)
		{
			FallthruAnalysis fa;
			if (!fa.exitEarly(body_.get()))
			{
				throw std::logic_error("missing a return statement");
			}
		}
	}

    std::string Function::fullName() const
    {
        return scope_->getFullName() + "." + name_;
    }

    /// CreateEntryBlockAlloca - Create an alloca instruction in the entry block of
    /// the function.  This is used for mutable variables etc.
    static llvm::AllocaInst *CreateEntryBlockAlloca(CodeGenContext& ctx, llvm::Function *f, const Variable* v)
    {
        llvm::IRBuilder<> TmpB(&f->getEntryBlock(), f->getEntryBlock().begin());

        const std::string vName = v->scope()->getFullName() + "." + v->name();

        return TmpB.CreateAlloca(v->type()->getllvm(ctx), 0, vName);
    }

    void Function::generateVariables(const Scope* scope, EnvCodeGenCtx& ectx) const
    {
        for (const auto& v : scope->variables_)
        {
			if (v.second->type()->is_a<StructType>() || v.second->type()->is_a<ArrayType>())
			{
				// Struct and array variables are by definition defined
				// That't not quite right but will do for now
				ectx.definedVariables_.insert({ v.second.get(), defined });

			}
			if (!v.second->isssa())
			{
				// Create an alloca for this variable.
				llvm::AllocaInst *Alloca = CreateEntryBlockAlloca(ectx.ctx_, ectx.function_, v.second.get());

				ectx.addVariable(v.second.get(), Alloca);

				// Set to undef for linear types
				if (v.second->type()->linear())
				{
					v.second->type()->generateUndef(ectx, Alloca);
				}
			}
        }

        for (auto& c : scope->children_)
        {
            generateVariables(c.get(), ectx);
        }
    }

    llvm::Function* Function::generateProto(CodeGenContext& ctx)
    {
		if (intrinsicId_)
		{
			llvm::Function* f;
			if (returnType_ && returnType_->is_a<StructType>())
			{
				auto structType = returnType_->is_a<StructType>();
				auto returnType = structType->fieldType(0)->getllvm(ctx);
				f = llvm::Intrinsic::getDeclaration(ctx.module_.get(), (llvm::Intrinsic::ID)intrinsicId_, { returnType });
			}
			else
			{
				f = llvm::Intrinsic::getDeclaration(ctx.module_.get(), (llvm::Intrinsic::ID)intrinsicId_);
			}
			if (!f)
				throw std::logic_error("cannot create llvm function");

			return f;
		}
        llvm::Type* returnType = llvm::Type::getVoidTy(ctx.context_);

        if (returnType_)
            returnType = returnType_->getllvm(ctx);

        if (!llvm::FunctionType::isValidReturnType(returnType))
            throw std::logic_error("invalid return type");

        std::vector<llvm::Type*> params;
        for (auto v : parameters_)
        {
            llvm::Type* paramType = v->type()->getllvm(ctx);

            if (!llvm::FunctionType::isValidArgumentType(paramType))
                throw std::logic_error("invalid parameter type");

            params.push_back(paramType);
        }

        llvm::FunctionType* ft = llvm::FunctionType::get(returnType, params, false);
        if (!ft)
            throw std::logic_error("cannot create llvm function type");

        llvm::Function *f = llvm::Function::Create(ft, llvm::Function::ExternalLinkage, fullName(), ctx.module_.get());
        if (!f)
            throw std::logic_error("cannot create llvm function");

		if (!body_)
		{
			std::string indirectName{ "__ind_" };
			indirectName.append(fullName());

			globalVar_ = new llvm::GlobalVariable(*ctx.module_, llvm::PointerType::getUnqual(ft), false, llvm::GlobalValue::ExternalLinkage, f, indirectName);
		}

		if (const_)
		{
			f->setOnlyReadsMemory();
		}

		if (returnType_ && returnType_->linear() && returnType_->is_a<PointerType>())
		{
			f->addAttribute(0, llvm::Attribute::NonNull);
		}
		unsigned counter = 1;
		for (auto v : parameters_)
		{
			auto paramType = v->type();
			if (paramType && paramType->linear() && paramType->is_a<PointerType>())
			{
				f->addAttribute(counter, llvm::Attribute::NonNull);
			}
			++counter;
		}
		llvmfn_ = f;

        return f;
    }

    void Function::generate(CodeGenContext& ctx) const
    {
		//DebugPrinter p(std::cout);
		//p.print(const_cast<Function*>(this));

		llvm::Function* f = ctx.module_->getFunction(fullName());
	
		// intrinsic function
		if (intrinsicId_)
			return;

		// external function
		if (!body_)
		{
			if (builtin_)
			{
				ctx.addNativeFunction(f, builtin_);
			}
			else
			{
				throw std::logic_error("unknown builtin function " + name_);
			}
			return;
		}

        EnvCodeGenCtx ectx(ctx, *this, f);

        // Create a new basic block to start insertion into.
        llvm::BasicBlock *BB = llvm::BasicBlock::Create(ctx.context_, "entry", f);
        ectx.builder_.SetInsertPoint(BB);

		// Set names for all arguments.
        size_t Idx = 0;
        for (auto& p : f->args())
        {
			// Function parameters are by definition defined
			ectx.definedVariables_.insert({ parameters_[Idx], defined });

            p.setName(parameters_[Idx]->name());
        
			if (parameters_[Idx]->isssa())
			{
				ectx.addVariable(parameters_[Idx], &p);
			}
			else
			{
				// Create an alloca for this variable.
				llvm::AllocaInst *Alloca = CreateEntryBlockAlloca(ctx, f, parameters_[Idx]);

				// Store the initial value into the alloca.
				ectx.builder_.CreateStore(&p, Alloca);

				ectx.addVariable(parameters_[Idx], Alloca);
			}
            ++Idx;
        }

        // add variables from visible scopes
        for (auto& c : paramScope_->children_)
        {
            generateVariables(c.get(), ectx);
        }

		llvm::BasicBlock *BBody = llvm::BasicBlock::Create(ctx.context_, "body", f);
        ectx.builder_.CreateBr(BBody);
        ectx.builder_.SetInsertPoint(BBody);

		auto result = body_->generate(ectx);
		
		if (!result.exited())
			throw std::logic_error("missing return statement");

		BBody = ectx.builder_.GetInsertBlock();
		if (llvm::pred_empty(BBody))
		{
			//nobody reached the end of function
			BBody->removeFromParent();
		}
		else
		{
			if (ectx.isFallThru(BBody))
			{
				// return ret for void functions
				if (returnType_)
				{
					throw std::logic_error("missing return statement");
				}
				ectx.builder_.CreateRetVoid();
			}
		}

		//ctx.module_->dump();
		//f->dump();

		if (llvm::verifyFunction(*f, &llvm::errs()))
		{
			DebugPrinter p(std::cout);
			p.print(const_cast<Function*>(this));
			f->dump();
			throw std::logic_error("compilation failed");
		}
    }

}