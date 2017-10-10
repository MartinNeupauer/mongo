#include "mongo/db/codegen/anta/EnvCodeGenCtx.h"
#include "mongo/db/codegen/anta/Statement.h"
#include "mongo/db/codegen/anta/Expr.h"
#include "mongo/db/codegen/anta/Function.h"
#include "mongo/db/codegen/anta/DebugPrint.h"

#include <llvm/IR/Module.h>

#include <iostream>
namespace anta
{
	Statements::Statements(std::vector<intrusive_ptr<Statement>> stmts) : stmts_(std::move(stmts))
	{
		if (stmts_.empty())
			throw std::logic_error("statement list must not be empty");

		for (const auto& s : stmts_)
		{
			if (!s)
				throw std::logic_error("statement list must not contain null");
		}
	}
	StmtGenResult Statements::generate(EnvCodeGenCtx& ectx)
    {
        // simply loop over all statements
		StmtGenResult result{ false };
		unsigned counter = 0;
        for (auto& stmt : stmts_)
        {
            result = stmt->generate(ectx);
			if (result.exited() && counter != stmts_.size() - 1)
			{
				std::cout << "****************************************\n";
				std::cout << "generating dead code at position " << counter << "\n";
				DebugPrinter p(std::cout);
				visit(p);
				std::cout << "****************************************\n";

				throw std::logic_error("unreachable code beyond this point");
			}
			++counter;
        }

		return result;
    }

	void Statements::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void Statements::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		for (auto& s : stmts_)
			s->preorder(v);

		v.popStmt();
	}

	void Statements::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		for (auto& s : stmts_)
			s->postorder(v);
		
		v.visit(this);
		v.popStmt();
	}

	const Statement* Statements::last() const
	{
		for(auto it = stmts_.rbegin(); it != stmts_.rend(); ++it)
		{
			auto l = (*it)->last();
			if (l)
				return l;
		}
		return nullptr;
	}

	static std::map<const Variable*, VariableState> mergeRanges(const std::map<const Variable*, VariableState>&lhs, const std::map<const Variable*, VariableState>&rhs)
	{
		std::map<const Variable*, VariableState> result;

		auto leftIt = lhs.begin();
		auto rightIt = rhs.begin();

		while (leftIt != lhs.end() && rightIt != rhs.end())
		{
			if (leftIt->first == rightIt->first)
			{
				if (leftIt->second == rightIt->second)
				{
					result.insert(*leftIt);
				}
				else
				{
					result.insert({leftIt->first, divergent});
				}
				++leftIt;
				++rightIt;
			}
			else if (leftIt->first < rightIt->first)
			{
				result.insert({ leftIt->first, divergent });
				++leftIt;
			}
			else
			{
				result.insert({ rightIt->first, divergent });
				++rightIt;
			}
		}

		while(leftIt != lhs.end())
		{
			result.insert({ leftIt->first, divergent });
			++leftIt;
		}
		while (rightIt != rhs.end())
		{
			result.insert({ rightIt->first, divergent });
			++rightIt;
		}

		return result;
	}

	IfStmt::IfStmt(const intrusive_ptr<const Expr>& condition, const intrusive_ptr<Statement>& then, const intrusive_ptr<Statement>& else__)
		: condition_(condition), then_(then), else_(else__)
	{
		if (!then_)
			throw std::logic_error("then branch must be set");
		if (!else_)
			throw std::logic_error("else branch must be set");
	}

	StmtGenResult IfStmt::generate(EnvCodeGenCtx& ectx)
    {
        llvm::Value* cond = condition_->generateTopLevel(ectx);
		
		if (!condition_->type()->is_a<Int1Type>())
		{
			// Convert condition to a bool by comparing equal to 0.
			cond = ectx.builder_.CreateICmpNE(cond, llvm::Constant::getNullValue(cond->getType()), "ifcondition");
		}

        // Create blocks for the then and else cases.  Insert the 'then' block at the
        // end of the function.
        llvm::BasicBlock *ThenBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "then", ectx.function_);
        llvm::BasicBlock *ElseBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "else");
        llvm::BasicBlock *MergeBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "ifmerge");

        ectx.builder_.CreateCondBr(cond, ThenBB, ElseBB);

        // Emit then value.
        ectx.builder_.SetInsertPoint(ThenBB);

		// Remember defined variables
		auto thenVariables = ectx.definedVariables_;

		auto resultThen = then_->generate(ectx);

		// Don't bother emitting the branch if we don't reach this point
		if (!resultThen.exited())
			ectx.CreateBr(MergeBB);
    
		// Swap back - thenVariables how holds variables defined in the then branch
		// and ectx.definedVariables_ now holds variables defined at the begining of the if statement
		std::swap(thenVariables, ectx.definedVariables_);

		// Emit else block.
        ectx.function_->getBasicBlockList().push_back(ElseBB);
        ectx.builder_.SetInsertPoint(ElseBB);

		auto resultElse = else_->generate(ectx);

		// Don't bother emitting the branch if we don't reach this point
		if (!resultElse.exited())
			ectx.CreateBr(MergeBB);

		bool bothExited = resultThen.exited() && resultElse.exited();

		if (bothExited)
		{
			delete MergeBB;
		} 
		else
		{
			// The if statement will continue the execution because at least one branch did not exit

			if (resultThen.exited())
			{
				// The else branch continues
				// do nothing - defined variables are already in ectx
			}
			else if (resultElse.exited())
			{
				// The then branch continues
				// Swap back - the results of the then branch
				std::swap(thenVariables, ectx.definedVariables_);
			}
			else
			{
				// If both continue then they must have the same set of defined variables
				if (thenVariables != ectx.definedVariables_)
				{
					ectx.definedVariables_ = mergeRanges(thenVariables, ectx.definedVariables_);
				}
			}

			// Emit merge block.
			ectx.function_->getBasicBlockList().push_back(MergeBB);
			ectx.builder_.SetInsertPoint(MergeBB);
		}

		return StmtGenResult{ bothExited };
	}

	void IfStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void IfStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		condition_->preorder(v);
		then_->preorder(v);
		else_->preorder(v);

		v.popStmt();
	}

	void IfStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		condition_->postorder(v);
		then_->postorder(v);
		else_->postorder(v);
		
		v.visit(this);
		v.popStmt();
	}

	FuncCallStmt::FuncCallStmt(const Function * function, std::vector<intrusive_ptr<const Expr>> args)
		: function_(function)
		, arguments_(std::move(args))
	{
		if (function_->parameters().size() != arguments_.size())
			throw std::logic_error("arguments and parameters do not match");

		for (unsigned i = 0; i < arguments_.size(); ++i)
		{
			if (arguments_[i]->type() != function_->parameters()[i]->type())
				throw std::logic_error("argument and parameter types do not match");
		}
	}

	StmtGenResult FuncCallStmt::generate(EnvCodeGenCtx& ectx)
    {
        std::vector<llvm::Value*> args;

        for (auto& e : arguments_)
        {
            args.push_back(e->generateTopLevel(ectx));
        }
		ectx.CreateCall(function_, args);

		return StmtGenResult{ false };
    }

	void FuncCallStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void FuncCallStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		for (auto& e : arguments_)
			e->preorder(v);
		v.popStmt();
	}

	void FuncCallStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		for (auto& e : arguments_)
			e->postorder(v);

		v.visit(this);
		v.popStmt();
	}

	AssignStmt::AssignStmt(const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs) 
		: lhs_(lhs), rhs_(rhs) 
	{
		if (lhs_->type() != rhs_->type())
			throw std::logic_error("invalid type");
	}

	StmtGenResult AssignStmt::generate(EnvCodeGenCtx& ectx)
    {
		auto var = lhs_->is_a<VarExpr>();
		if (var && var->isssa())
		{
			ectx.definedVariables_[var->var()] = defined;
			llvm::Value* rhs = rhs_->generateTopLevel(ectx);
			ectx.addVariable(var->var(), rhs);
		}
		else
		{
			if (!lhs_->lvalue())
				throw std::logic_error("lhs must be lvalue");

			llvm::Value* rhs = rhs_->generateTopLevel(ectx);
			llvm::Value* lhs = lhs_->generateRefTopLevel(ectx);

			//ectx.builder_.CreateStore(rhs, lhs);
			ectx.builder_.CreateAlignedStore(rhs, lhs, lhs_->type()->alignSize());
		}

		return StmtGenResult{ false };
    }

	void AssignStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void AssignStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		lhs_->preorder(v);
		rhs_->preorder(v);
		v.popStmt();
	}

	void AssignStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		lhs_->postorder(v);
		rhs_->postorder(v);

		v.visit(this);
		v.popStmt();
	}

	ReturnStmt::ReturnStmt(const intrusive_ptr<const Expr>& expr) 
		: return_(expr) {}

	StmtGenResult ReturnStmt::generate(EnvCodeGenCtx& ectx)
    {
        if (return_)
        {
			llvm::Value* retVal = return_->generateTopLevel(ectx);
			generateKill(ectx);
			ectx.builder_.CreateRet(retVal);
        }
		else
		{
			generateKill(ectx);
			ectx.builder_.CreateRetVoid();
		}

		return StmtGenResult{ true };
    }

	void ReturnStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void ReturnStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		if (return_) return_->preorder(v);
		v.popStmt();
	}

	void ReturnStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		if (return_) return_->postorder(v);
		
		v.visit(this);
		v.popStmt();
	}

	StmtGenResult BreakStmt::generate(EnvCodeGenCtx& ectx)
    {
		if (!valid() || level_ >= ectx.blockBreak_.size())
			throw std::logic_error("break is out of range");

		auto& b = *(ectx.blockBreak_.rbegin() + level_);
		if (b.hasBreak_)
		{
			b.definedVariables_ = mergeRanges(b.definedVariables_, ectx.definedVariables_);
		}
		else
		{
			b.definedVariables_ = ectx.definedVariables_;
		}
		b.hasBreak_ = true;
		ectx.CreateBr(b.block_);

		return StmtGenResult{ true };
    }

	void BreakStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void BreakStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);
		v.popStmt();
	}

	void BreakStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);
		v.popStmt();
	}

	StmtGenResult ContinueStmt::generate(EnvCodeGenCtx & ectx)
	{
		throw std::logic_error("continue must be lowered");
	}

	void ContinueStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void ContinueStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);
		v.popStmt();
	}

	void ContinueStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);
		v.popStmt();
	}

	SwitchStmt::SwitchStmt(const intrusive_ptr<const Expr>& expr, std::vector<intrusive_ptr<Statement>> branches)
		: expr_(expr), branches_(std::move(branches))
	{
		for (auto b : branches_)
		{
			if (!b)
				throw std::logic_error("branch must not be null");
		}
	}

	StmtGenResult SwitchStmt::generate(EnvCodeGenCtx& ectx)
	{
		llvm::Value* val = expr_->generateTopLevel(ectx);

		llvm::BasicBlock *mergeBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "switchmerge");
		llvm::BasicBlock *defaultBB = llvm::BasicBlock::Create(ectx.ctx_.context_, "switchdefault");

		llvm::SwitchInst* sw = ectx.builder_.CreateSwitch(val, defaultBB, branches_.size());

		auto saveVariables = ectx.definedVariables_;
		std::map<const Variable*, VariableState> mergedVariables;

		bool firstBranch = true;
		bool allExited = true;
		for (unsigned i = 0; i < branches_.size(); ++i)
		{
			llvm::BasicBlock *branchBB = llvm::BasicBlock::Create(ectx.ctx_.context_, std::string{ "switchbranch" }.append(std::to_string(i)), ectx.function_);
			ectx.builder_.SetInsertPoint(branchBB);

			sw->addCase(llvm::ConstantInt::get(llvm::Type::getInt32Ty(ectx.ctx_.context_), i), branchBB);

			ectx.definedVariables_ = saveVariables;

			// actually generate the branch
			auto result = branches_[i]->generate(ectx);

			if (!result.exited())
			{
				// the branch will fallthrou
				if (firstBranch)
				{
					firstBranch = false;
					mergedVariables = ectx.definedVariables_;
				}
				else
				{
					mergedVariables = mergeRanges(mergedVariables, ectx.definedVariables_);
				}
			}
			allExited = allExited & result.exited();

			// and branch to the merge point
			if (!result.exited())
				ectx.CreateBr(mergeBB);
		}

		ectx.function_->getBasicBlockList().push_back(defaultBB);
		ectx.builder_.SetInsertPoint(defaultBB);
		ectx.builder_.CreateUnreachable();

		if (allExited)
		{
			delete mergeBB;
		}
		else
		{
			ectx.definedVariables_ = mergedVariables;
			ectx.function_->getBasicBlockList().push_back(mergeBB);
			ectx.builder_.SetInsertPoint(mergeBB);
		}

		return StmtGenResult{ allExited };
	}

	void SwitchStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void SwitchStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		expr_->preorder(v);
		for (auto& s : branches_)
			s->preorder(v);
		v.popStmt();
	}

	void SwitchStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		expr_->postorder(v);
		for (auto& s : branches_)
			s->postorder(v);

		v.visit(this);
		v.popStmt();
	}

	 MFuncCallStmt::MFuncCallStmt(const Function * function, std::vector<intrusive_ptr<const Expr>> args, std::vector<FuncletDef> branches)
		: function_(function), arguments_(std::move(args)), branches_(std::move(branches))
	{
		 // skip the 'hidden' first parameter(i.e. +1)
		 if (function_->parameters().size() != arguments_.size()+1)
			 throw std::logic_error("arguments and parameters do not match");
	
		 for (unsigned i = 0; i < arguments_.size(); ++i)
		 {
			 if (arguments_[i]->type() != function_->parameters()[i+1]->type())
				 throw std::logic_error("argument and parameter types do not match");
		 }

		 if (function_->returns().size() != branches_.size())
			 throw std::logic_error("returns and branches do not match");

		 for (auto& b : branches_)
		 {
			 if (!b.stmt_)
				 throw std::logic_error("branch must have a statement");
		 }
	 }

	 StmtGenResult MFuncCallStmt::generate(EnvCodeGenCtx & ectx)
	 {
		 throw std::logic_error("mfunccall must be lowered");
	 }

	 void MFuncCallStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void MFuncCallStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		for (auto& e : arguments_)
			e->preorder(v);

		for (auto& b : branches_)
		{
			for (auto& e : b.returnExprs_)
				e->preorder(v);
			
			b.stmt_->preorder(v);
		}
		v.popStmt();
	}

	void MFuncCallStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		for (auto& e : arguments_)
			e->postorder(v);

		for (auto& b : branches_)
		{
			for (auto& e : b.returnExprs_)
				e->postorder(v);
			
			b.stmt_->postorder(v);
		}
		
		v.visit(this);
		v.popStmt();
	}

	StmtGenResult MReturnStmt::generate(EnvCodeGenCtx & ectx)
	{
		throw std::logic_error("mreturn must be lowered");
	}

	void MReturnStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void MReturnStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		for (auto& e : return_.returnExprs_)
			e->preorder(v);
		v.popStmt();
	}

	void MReturnStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		for (auto& e : return_.returnExprs_)
			e->postorder(v);

		v.visit(this);
		v.popStmt();
	}

	void Statement::generateKill(EnvCodeGenCtx & ectx)
	{
		for (auto v : ectx.definedVariables_)
		{
			if (v.first->type()->linear() && v.second != used)
			{
				llvm::Value* value = ectx.getVariable(v.first);
				value = v.first->isssa() ? value : ectx.builder_.CreateLoad(value);
				v.first->type()->generateKill(ectx, value);
			}
		}
	}

	BlockStmt::BlockStmt(const intrusive_ptr<Statement>& body, bool loop)
		: body_(body), loop_(loop)
	{
		if (!body_)
			throw std::logic_error("body must be set in the block");
	}

	StmtGenResult BlockStmt::generate(EnvCodeGenCtx & ectx)
	{
		llvm::BasicBlock *Block = llvm::BasicBlock::Create(ectx.ctx_.context_, "blockbegin", ectx.function_);
		llvm::BasicBlock *AfterBlock = llvm::BasicBlock::Create(ectx.ctx_.context_, "blockend");

		// Set the break BB
		ectx.blockBreak_.push_back(BlockBreak{ AfterBlock, {}, false });

		// Insert an explicit fall through from the current block to the Block.
		ectx.CreateBr(Block);

		// Start insertion in Block.
		ectx.builder_.SetInsertPoint(Block);

		// Emit body block.
		auto result = body_->generate(ectx);

		if (ectx.blockBreak_.back().hasBreak_)
		{
			if (result.exited())
			{
				// only the "jump" from a break continues
				ectx.definedVariables_ = ectx.blockBreak_.back().definedVariables_;
			}
			else
			{
				// both the body and the "jump" from a break continues
				ectx.definedVariables_ = mergeRanges(ectx.blockBreak_.back().definedVariables_, ectx.definedVariables_);
			}
			result.exited_ = false;
		}
		else
		{
			if (loop())
			{
				// this is an unconditional infinite loop
				result.exited_ = true;
			}
		}

		ectx.blockBreak_.pop_back();

		if (loop())
		{
			ectx.CreateBr(Block);
		}

		if (result.exited())
		{
			delete AfterBlock;
		}
		else
		{
			ectx.CreateBr(AfterBlock);

			// Emit after block.
			ectx.function_->getBasicBlockList().push_back(AfterBlock);
			ectx.builder_.SetInsertPoint(AfterBlock);
		}

		return result;
	}

	void BlockStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void BlockStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);

		body_->preorder(v);

		v.popStmt();
	}

	void BlockStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);

		body_->postorder(v);

		v.visit(this);
		v.popStmt();
	}

	StmtGenResult NoopStmt::generate(EnvCodeGenCtx & ectx)
	{
		return StmtGenResult{ false };
	}

	void NoopStmt::visit(AbstractVisitor & v)
	{
		v.visit(this);
	}

	void NoopStmt::preorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);
		v.popStmt();
	}

	void NoopStmt::postorder(AbstractVisitor & v)
	{
		v.pushStmt(this);
		v.visit(this);
		v.popStmt();
	}
}