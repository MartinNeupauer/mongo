#include "mongo/db/codegen/anta/Analysis.h"
#include "mongo/db/codegen/anta/Statement.h"
#include "mongo/db/codegen/anta/Function.h"

#include <iostream>

namespace anta
{
	bool FallthruAnalysis::exitEarly(Statement * stmt)
	{
		state_ = Fallthru;

		stmt->visit(*this);

		return state_ == Exit;
	}
	bool FallthruAnalysis::exitEarly(const std::vector<intrusive_ptr<Statement>>& stmts)
	{
		state_ = Fallthru;

		visit(stmts);

		return state_ == Exit;
	}
	void FallthruAnalysis::visit(const std::vector<intrusive_ptr<Statement>>& stmts)
	{
		size_t size = stmts.size() - 1;
		size_t position = 0;

		for (auto& s : stmts)
		{
			s->visit(*this);

			// only the last statement may exit
			if (state_ == Exit && position != size)
			{
				throw std::logic_error("unreachable code beyond this point");
			}
			++position;
		}
	}
	void FallthruAnalysis::visit(NoopStmt *)
	{
		state_ = Fallthru;
	}
	void FallthruAnalysis::visit(Statements *stmt)
	{
		visit(stmt->stmts());
	}
	void FallthruAnalysis::visit(IfStmt *stmt)
	{
		state_ = Fallthru;
		stmt->then()->visit(*this);

		State thenState = state_;
		State elseState = Fallthru;

		// else branch
		stmt->elseB()->visit(*this);
		elseState = state_;

		state_ = (thenState == Exit && elseState == Exit) ? Exit : Fallthru;
	}
	void FallthruAnalysis::visit(FuncCallStmt *)
	{
		state_ = Fallthru;
	}
	void FallthruAnalysis::visit(AssignStmt *)
	{
		state_ = Fallthru;
	}
	void FallthruAnalysis::visit(ReturnStmt *)
	{
		state_ = Exit;
	}
	void FallthruAnalysis::visit(BreakStmt *stmt)
	{
		if (blockBreak_.size() > stmt->level())
		{
			(blockBreak_.rbegin() + stmt->level())->hasBreak_ = true;
		}
		state_ = Exit;
	}
	void FallthruAnalysis::visit(ContinueStmt *)
	{
		throw std::logic_error("continue must be lowered");
	}
	void FallthruAnalysis::visit(SwitchStmt *stmt)
	{
		bool allExit = true;
		for (auto& s : stmt->branches())
		{
			s->visit(*this);
			allExit = allExit && (state_ == Exit);
		}

		state_ = allExit ? Exit : Fallthru;
	}
	void FallthruAnalysis::visit(MFuncCallStmt *stmt)
	{
		bool allExit = true;
		for (auto& b : stmt->branches())
		{
			state_ = Fallthru;
			b.stmt_->visit(*this);

			allExit = allExit && (state_ == Exit);
		}

		state_ = allExit ? Exit : Fallthru;
	}
	void FallthruAnalysis::visit(MReturnStmt *)
	{
		state_ = Exit;
	}
	void FallthruAnalysis::visit(BlockStmt *stmt)
	{
		state_ = Fallthru;
		blockBreak_.push_back(BlockBreak{stmt, false});

		stmt->body()->visit(*this);

		bool hasBreak = blockBreak_.back().hasBreak_;

		blockBreak_.pop_back();

		if (hasBreak)
		{
			state_ = Fallthru;
		}
		else
		{
			if (stmt->loop())
			{
				state_ = Exit; // this is an unconditional infinite loop
			}
		}
	}
	BlockStmt* RemoveRedundantBlocks::findBlock(unsigned level)
	{
		for (auto it = stmtStack_.rbegin(); it != stmtStack_.rend(); ++it)
		{
			auto block = (*it)->is_a<BlockStmt>();

			if (block)
			{
				if (level == 0)
					return block;

				--level;
			}
		}

		throw std::logic_error("break is out of range");
	}

	void RemoveRedundantBlocks::run()
	{
		fn_.body_->postorder(*this);
		replace(fn_.body_);
	}
	void RemoveRedundantBlocks::visit(BreakStmt *stmt)
	{
		// already replaced
		if (stmtMap_.count(stmt)) return;

		Replacer::visit(stmt);

		breakMap_.insert({ stmt, findBlock(stmt->level()) });
	}
	void RemoveRedundantBlocks::visit(ContinueStmt *)
	{
		throw std::logic_error("continue must be lowered");
	}
	void RemoveRedundantBlocks::visit(BlockStmt *stmt)
	{
		// already replaced
		if (stmtMap_.count(stmt)) return;

		Replacer::visit(stmt);

		bool canCollapse = true;
		auto inner = stmt->body()->is_a<BlockStmt>();
		if (!inner)
			canCollapse = false;

		if (canCollapse && (stmt->loop() || inner->loop()))
			canCollapse = false;

		// Two blocks can be collapsed 
		if (canCollapse)
		{
			for (auto& b : breakMap_)
			{
				if (b.first->level() > 0)
					--b.first->level();
			}

			stmtMap_.insert({ stmt, inner });
		}

		// remove all break statements that are below this statement
		for (auto it = breakMap_.begin(); it != breakMap_.end();)
		{
			if (it->second == stmt)
			{
				it = breakMap_.erase(it);
			}
			else
			{
				++it;
			}
		}

	}
}