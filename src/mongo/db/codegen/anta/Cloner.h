#pragma once
#include "Visitor.h"

#include <boost/intrusive_ptr.hpp>
#include <unordered_map>

namespace anta
{
	using namespace boost;
	class Cloner : public AbstractVisitor
	{
	protected:
		SemaFactory& f_;
		Function& callerFn_;

		std::unordered_map<const Scope*, Scope*> scopeMap_;
		std::unordered_map<const Variable*, Variable*> variableMap_;
		std::unordered_map<const Expr*, intrusive_ptr<const Expr>> exprMap_;
		std::unordered_map<const Statement*, intrusive_ptr<Statement>> stmtMap_;

		Scope* clone(const Scope* scope);
		Variable* clone(const Variable* var);

	public:
		Cloner(
			SemaFactory& f, 
			Function& fn,
			const std::unordered_map<const Scope*, Scope*>& scopeMap = {},
			const std::unordered_map<const Variable*, Variable*>& variableMap = {},
			const std::unordered_map<const Expr*, intrusive_ptr<const Expr>>& exprMap = {},
			const std::unordered_map<const Statement*, intrusive_ptr<Statement>>& stmtMap = {}
		)
			: f_(f), callerFn_(fn), scopeMap_(scopeMap), variableMap_(variableMap), exprMap_(exprMap), stmtMap_(stmtMap)
		{
			//
			// Pulvis es et in pulverem reverteris.
			//
			scopeMap_.insert({ nullptr,nullptr });
			variableMap_.insert({ nullptr,nullptr });
			exprMap_.insert({ nullptr,nullptr });
			stmtMap_.insert({ nullptr,nullptr });
		}

		intrusive_ptr<Statement> clone(Statement* stmt);

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