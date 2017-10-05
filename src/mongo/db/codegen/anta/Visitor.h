#pragma once

#include "mongo/db/codegen/anta/Sema.h"

#include <vector>

namespace anta
{
	class AbstractVisitor
	{
	protected:
		std::vector<Statement*> stmtStack_;
	public:

		void pushStmt(Statement* s) { stmtStack_.push_back(s); }
		void popStmt() { stmtStack_.pop_back(); }

		virtual void visit(NoopStmt*) = 0;
		virtual void visit(Statements*) = 0;
		virtual void visit(IfStmt*) = 0;
		virtual void visit(FuncCallStmt*) = 0;
		virtual void visit(AssignStmt*) = 0;
		virtual void visit(ReturnStmt*) = 0;
		virtual void visit(BreakStmt*) = 0;
		virtual void visit(ContinueStmt*) = 0;
		virtual void visit(SwitchStmt*) = 0;
		virtual void visit(MFuncCallStmt*) = 0;
		virtual void visit(MReturnStmt*) = 0;
		virtual void visit(BlockStmt*) = 0;

		virtual void visit(const UnaryExpr*) = 0;
		virtual void visit(const BinaryExpr*) = 0;
		virtual void visit(const StructAccessExpr*) = 0;
		virtual void visit(const PointerArithmExpr*) = 0;
		virtual void visit(const DereferenceExpr*) = 0;
		virtual void visit(const AddressOfExpr*) = 0;
		virtual void visit(const VarExpr*) = 0;
		virtual void visit(const ConstExpr*) = 0;
		virtual void visit(const BitCastExpr*) = 0;
		virtual void visit(const FuncCallExpr*) = 0;
		virtual void visit(const HoleExpr*) = 0;
		virtual void visit(const InlinedFuncExpr*) = 0;
	};

	class DefaultVisitor : public AbstractVisitor
	{
	public:
		using AbstractVisitor::visit;

		virtual void visit(NoopStmt*) override {}
		virtual void visit(Statements*) override {}
		virtual void visit(IfStmt*) override {}
		virtual void visit(FuncCallStmt*) override {}
		virtual void visit(AssignStmt*) override {}
		virtual void visit(ReturnStmt*) override {}
		virtual void visit(BreakStmt*) override {}
		virtual void visit(ContinueStmt*) override {}
		virtual void visit(SwitchStmt*) override {}
		virtual void visit(MFuncCallStmt*) override {}
		virtual void visit(MReturnStmt*) override {}
		virtual void visit(BlockStmt*) override {}

		virtual void visit(const UnaryExpr*) override {}
		virtual void visit(const BinaryExpr*) override {}
		virtual void visit(const StructAccessExpr*) override {}
		virtual void visit(const PointerArithmExpr*) override {}
		virtual void visit(const DereferenceExpr*) override {}
		virtual void visit(const AddressOfExpr*) override {}
		virtual void visit(const VarExpr*) override {}
		virtual void visit(const ConstExpr*) override {}
		virtual void visit(const BitCastExpr*) override {}
		virtual void visit(const FuncCallExpr*) override {}
		virtual void visit(const HoleExpr*) override {}
		virtual void visit(const InlinedFuncExpr*) override {}
	};
}