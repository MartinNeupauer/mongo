#pragma once

#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/CodeGenCtx.h"
#include "mongo/db/codegen/anta/Variable.h"

#include <llvm/IR/IRBuilder.h>

#include <map>
#include <set>

namespace llvm
{
    class Function;
}

namespace anta
{
	struct TypedValue
	{
		llvm::Value* value_;
		const Type* type_;
	};

	struct TypeValueHasher
	{
		std::size_t operator() (const anta::TypedValue& v) const
		{
			return std::hash<void*>()(v.value_) ^ std::hash<const void*>()(v.type_);
		}
	};

	enum VariableState : unsigned
	{
		defined,
		used, // linear types only
		divergent
	};

	struct StmtGenResult
	{
		bool exited_;

		bool exited() const { return exited_; }
	};

	struct BlockBreak
	{
		llvm::BasicBlock* block_;
		std::map<const Variable*, VariableState>       definedVariables_;
		bool hasBreak_;
	};

	// This is a per function context
    struct EnvCodeGenCtx
    {
        CodeGenContext&                           ctx_;
		const Function&                           fn_;
        llvm::Function*                           function_;
        std::map<const Variable*, llvm::Value*>   variables_;
        llvm::IRBuilder<>                         builder_;
		std::vector<BlockBreak>                   blockBreak_;
		std::vector<TypedValue>                   exprKillSet_;
		std::map<const Variable*, VariableState>       definedVariables_;

        EnvCodeGenCtx(CodeGenContext& ctx, const Function& fn, llvm::Function* func) : ctx_(ctx), fn_(fn), function_(func), builder_(ctx.context_) {}

		bool isFallThru(const llvm::BasicBlock* BB) const
		{
			if (BB->empty() || !llvm::isa<llvm::TerminatorInst>(BB->back()))
				return true;

			return false;
		}

		llvm::Value* CreateBr(llvm::BasicBlock *dest)
		{
			llvm::BasicBlock* BB = builder_.GetInsertBlock();
			if (isFallThru(BB))
				return builder_.CreateBr(dest);

			return &BB->back();
		}

		llvm::Value* CreateCall(const Function* f, llvm::ArrayRef<llvm::Value *> args = llvm::None);

		bool isDefined(const Variable* var) const
		{
			auto it = definedVariables_.find(var);
			if (it == definedVariables_.end())
				return false;

			return it->second == defined;
		}

		bool isDivergent(const Variable* var) const
		{
			auto it = definedVariables_.find(var);
			if (it == definedVariables_.end())
				return false;

			return it->second == divergent;
		}

		void addVariable(const Variable* var, llvm::Value* val)
        {
            auto res = variables_.insert(std::make_pair(var, val));
            if (!res.second)
            {
                std::string errorMsg = "duplicate variable name " + var->name();
                throw std::logic_error(errorMsg);
            }
			val->setName(var->name());
        }

        llvm::Value* getVariable(const Variable* var) const
        {
            auto it = variables_.find(var);
            if (it == variables_.end())
            {
                throw std::logic_error("cannot find a variable " + var->name());
            }

            return it->second;
        }
    };
}
