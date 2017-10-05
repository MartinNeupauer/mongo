#pragma once

#include "Visitor.h"
#include "Statement.h"

#include <unordered_map>

namespace anta
{
	class Replacer : public AbstractVisitor
	{
	protected:
		SemaFactory& f_;

		std::unordered_map<const Statement*, intrusive_ptr<Statement>> stmtMap_;
		std::unordered_map<const Expr*, intrusive_ptr<const Expr>> exprMap_;

		bool replace(intrusive_ptr<const Expr>&) const;
		bool replace(intrusive_ptr<Statement>&) const;
		bool replace(std::vector<intrusive_ptr<const Expr>>&) const;
		bool replace(std::vector<intrusive_ptr<Statement>>&) const;
		
		public:
		Replacer(
			SemaFactory& f,
			const std::unordered_map<const Statement*, intrusive_ptr<Statement>>& stmtMap = {}
		)
			: f_(f), stmtMap_(stmtMap)
		{
			//
			// Pulvis es et in pulverem reverteris.
			//
			stmtMap_.insert({ nullptr,nullptr });
			exprMap_.insert({ nullptr,nullptr });
		}

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