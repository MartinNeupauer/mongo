#pragma once

#include "mongo/db/codegen/anta/Visitor.h"
#include "mongo/db/codegen/anta/Statement.h"
#include "mongo/db/codegen/anta/ReplaceStmt.h"

#include <vector>
#include <unordered_map>

namespace anta
{
	class Function;

	class FallthruAnalysis : public DefaultVisitor
	{
		enum State
		{
			Unknown,
			Fallthru,
			Exit,
		};
		struct BlockBreak
		{
			BlockStmt* block_;
			bool hasBreak_;
		};

		State state_;
		std::vector<BlockBreak> blockBreak_;
	public:
		bool exitEarly(Statement* stmt);
		bool exitEarly(const std::vector<intrusive_ptr<Statement>>& stmts);

		void visit(const std::vector<intrusive_ptr<Statement>>& stmts);
	
		using DefaultVisitor::visit;

		virtual void visit(NoopStmt*) override;
		virtual void visit(Statements*) override;
		virtual void visit(IfStmt*) override;
		virtual void visit(FuncCallStmt*) override;
		virtual void visit(AssignStmt*) override;
		virtual void visit(ReturnStmt*) override;
		virtual void visit(BreakStmt*) override;
		virtual void visit(ContinueStmt*) override;
		virtual void visit(SwitchStmt*) override;
		virtual void visit(MFuncCallStmt*) override;
		virtual void visit(MReturnStmt*) override;
		virtual void visit(BlockStmt*) override;
	};

	class RemoveRedundantBlocks : public Replacer
	{
		Function& fn_;

		std::unordered_map<BreakStmt*, BlockStmt*> breakMap_;

		BlockStmt* findBlock(unsigned level);

	public:
		RemoveRedundantBlocks(SemaFactory& f, Function& fn) : Replacer(f), fn_(fn) {}

		void run();
	
		using Replacer::visit;
		virtual void visit(BreakStmt*) override;
		virtual void visit(ContinueStmt*) override;
		virtual void visit(BlockStmt*) override;
	};
}