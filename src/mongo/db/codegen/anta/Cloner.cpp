#include "mongo/db/codegen/anta/Cloner.h"

#include "mongo/db/codegen/anta/Factory.h"
#include "mongo/db/codegen/anta/Statement.h"

#include <vector>

namespace anta
{
	Scope * Cloner::clone(const Scope * scope)
	{
		if (scope == f_.globalScope())
			return f_.globalScope();

		auto it = scopeMap_.find(scope);
		if (it != scopeMap_.end())
			return it->second;

		Scope* cloned = f_.createScope(clone(scope->parent()));

		scopeMap_.insert({scope, cloned});
		return cloned;
	}

	Variable * Cloner::clone(const Variable * var)
	{
		auto it = variableMap_.find(var);
		if (it != variableMap_.end())
			return it->second;
		
		auto cloned = f_.createVariable(clone(var->scope()), var->type(), var->name(), var->isssa());

		variableMap_.insert({var, cloned});
		return cloned;
	}

	intrusive_ptr<Statement> Cloner::clone(Statement * stmt)
	{
		stmt->postorder(*this);

		auto it = stmtMap_.find(stmt);
		return it == stmtMap_.end() ? nullptr : it->second;
	}

	void Cloner::visit(NoopStmt *original)
	{
		if (stmtMap_.count(original)) return;

		auto cloned = f_.Noop();

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(Statements *original)
	{
		if (stmtMap_.count(original)) return;

		std::vector<intrusive_ptr<Statement>> stmts;
		for (auto& s : original->stmts())
			stmts.push_back(stmtMap_.find(s.get())->second);
		
		auto clone = f_.Stmts(std::move(stmts));

		stmtMap_.insert({ original, clone });
	}

	void Cloner::visit(IfStmt *original)
	{
		if (stmtMap_.count(original)) return;

		auto clone = f_.If(exprMap_.find(original->condition_.get())->second,
						   stmtMap_.find(original->then_.get())->second,
						   stmtMap_.find(original->else_.get())->second);

		stmtMap_.insert({ original, clone });
	}

	void Cloner::visit(FuncCallStmt *original)
	{
		if (stmtMap_.count(original)) return;

		std::vector<intrusive_ptr<const Expr>> arguments;
		for (auto& e : original->arguments_)
			arguments.push_back(exprMap_.find(e.get())->second);

		auto clone = f_.FuncStmt(original->function_, std::move(arguments));

		stmtMap_.insert({ original, clone });
	}

	void Cloner::visit(AssignStmt *original)
	{
		if (stmtMap_.count(original)) return;

		auto cloned = f_.Assign(exprMap_.find(original->lhs_.get())->second, exprMap_.find(original->rhs_.get())->second, original->lhs_->rvalue());

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(ReturnStmt *original)
	{
		if (stmtMap_.count(original)) return;

		auto cloned = f_.Return(exprMap_.find(original->return_.get())->second);

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(BreakStmt *original)
	{
		if (stmtMap_.count(original)) return;

		auto cloned = f_.Break(original->level());

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(ContinueStmt *original)
	{
		if (stmtMap_.count(original)) return;

		auto cloned = f_.Continue();

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(SwitchStmt *original)
	{
		if (stmtMap_.count(original)) return;

		std::vector<intrusive_ptr<Statement>> branches;
		for (auto& s : original->branches_)
			branches.push_back(stmtMap_.find(s.get())->second);

		auto cloned = f_.Switch(exprMap_.find(original->expr_.get())->second, std::move(branches));

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(MFuncCallStmt *original)
	{
		if (stmtMap_.count(original)) return;

		std::vector<intrusive_ptr<const Expr>> arguments;
		for (auto& e : original->arguments_)
			arguments.push_back(exprMap_.find(e.get())->second);

		std::vector<FuncletDef> branches;
		for (auto& b : original->branches_)
		{
			std::vector<intrusive_ptr<const Expr>> returnExprs;
			for (auto& e : b.returnExprs_)
				returnExprs.push_back(exprMap_.find(e.get())->second);

			branches.push_back({b.label_, std::move(returnExprs), stmtMap_.find(b.stmt_.get())->second});
		}

		auto cloned = f_.MFuncStmt(original->function_, std::move(arguments), std::move(branches));

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(MReturnStmt *original)
	{
		if (stmtMap_.count(original)) return;

		std::vector<intrusive_ptr<const Expr>> returnExprs;
		for (auto& e : original->return_.returnExprs_)
			returnExprs.push_back(exprMap_.find(e.get())->second);

		auto cloned = f_.MReturn({original->return_.label_, std::move(returnExprs)});

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(BlockStmt *original)
	{
		if (stmtMap_.count(original)) return;

		auto cloned = f_.Block(stmtMap_.find(original->body().get())->second, original->loop());

		stmtMap_.insert({ original, cloned });
	}

	void Cloner::visit(const UnaryExpr *original)
	{
		if (exprMap_.count(original)) return;

		auto cloned = f_.Unary(original->type(), original->op(), exprMap_.find(original->expr().get())->second);

		exprMap_.insert({original, cloned});
	}

	void Cloner::visit(const BinaryExpr *original)
	{
		if (exprMap_.count(original)) return;

		auto cloned = f_.Binary(original->type(), original->op(), exprMap_.find(original->left().get())->second, exprMap_.find(original->right().get())->second);

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const StructAccessExpr *original)
	{
		if (exprMap_.count(original)) return;

		// WRONG !!!
		auto cloned = make_intrusive<StructAccessExpr>(original->type(), exprMap_.find(original->ptr().get())->second, original->ordinal());

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const PointerArithmExpr *original)
	{
		if (exprMap_.count(original)) return;

		// WRONG !!!
		auto cloned = make_intrusive<PointerArithmExpr>(original->type(), original->op(), exprMap_.find(original->left().get())->second, exprMap_.find(original->right().get())->second);

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const DereferenceExpr *original)
	{
		if (exprMap_.count(original)) return;

		auto cloned = f_.Dereference(exprMap_.find(original->ptr().get())->second);

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const AddressOfExpr *original)
	{
		if (exprMap_.count(original)) return;

		auto cloned = f_.AddressOf(exprMap_.find(original->expr().get())->second);

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const VarExpr *original)
	{
		if (exprMap_.count(original)) return;

		auto cloned = f_.Var(clone(original->var()));

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const ConstExpr *original)
	{
		if (exprMap_.count(original)) return;

		// WRONG !!!
		auto cloned = make_intrusive<ConstExpr>(original->value());

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const BitCastExpr *original)
	{
		if (exprMap_.count(original)) return;

		auto cloned = f_.BitCast(exprMap_.find(original->expr().get())->second, original->type());

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const FuncCallExpr *original)
	{
		if (exprMap_.count(original)) return;

		std::vector<intrusive_ptr<const Expr>> arguments;
		for (const auto& e : original->arguments())
			arguments.push_back(exprMap_.find(e.get())->second);

		auto cloned = f_.FuncExpr(original->function(), std::move(arguments));

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const HoleExpr* original)
	{
		if (exprMap_.count(original)) return;
		// when cloning simply go ahead and replace the hole expr with 
		// an expression that the hole actually points to
		// we can do that as by the time we clone expressions
		// all known holes must be filled
		auto cloned = exprMap_.find(original->placeholder().get())->second;

		exprMap_.insert({ original, cloned });
	}

	void Cloner::visit(const InlinedFuncExpr *original)
	{
		if (exprMap_.count(original)) return;

		auto returnExpr = exprMap_.find(original->returnExpr().get())->second;
		auto body = stmtMap_.find(original->body().get())->second;

		auto cloned = f_.InlinedFunc(returnExpr, body);

		exprMap_.insert({ original, cloned });
	}
}