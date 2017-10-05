#pragma once

#include "Visitor.h"

#include <ostream>
namespace anta
{
	class DebugPrinter : public AbstractVisitor
	{
		std::ostream& os_;
		unsigned indent_;

		void indent()
		{
			for (unsigned i = 0; i < indent_; ++i)
				os_ << "    ";
		}
		void openbracket()
		{
			indent(); os_ << "{\n"; ++indent_;
		}
		void closebracket()
		{
			--indent_; indent(); os_ << "}\n";
		}

	public:
		DebugPrinter(std::ostream& os) : os_(os), indent_(0) {}

		void print(Function* fn);

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

		virtual void visit(const UnaryExpr*) override;
		virtual void visit(const BinaryExpr*) override;
		virtual void visit(const StructAccessExpr*) override;
		virtual void visit(const PointerArithmExpr*) override;
		virtual void visit(const DereferenceExpr*) override;
		virtual void visit(const AddressOfExpr*) override;
		virtual void visit(const VarExpr*) override;
		virtual void visit(const ConstExpr*) override;
		virtual void visit(const BitCastExpr*) override;
		virtual void visit(const FuncCallExpr*) override;
		virtual void visit(const HoleExpr*) override;
		virtual void visit(const InlinedFuncExpr*) override;
	};
}