#include "ReplaceStmt.h"
#include "Statement.h"
#include "Factory.h"

namespace anta
{
	bool Replacer::replace(intrusive_ptr<const Expr>& expr) const
	{
		if (expr)
		{
			auto replacement = exprMap_.find(expr.get());
			if (replacement != exprMap_.end())
			{
				expr = replacement->second;
				return true;
			}
		}

		return false;
	}

	bool Replacer::replace(intrusive_ptr<Statement>& stmt) const
	{
		if (stmt)
		{
			auto replacement = stmtMap_.find(stmt.get());
			if (replacement != stmtMap_.end())
			{
				stmt = replacement->second;
				return true;
			}
		}

		return false;
	}
	bool Replacer::replace(std::vector<intrusive_ptr<const Expr>>& exprs) const
	{
		bool replaced = false;

		for(auto& e : exprs)
		{
			replaced |= replace(e);
		}

		return replaced;
	}
	bool Replacer::replace(std::vector<intrusive_ptr<Statement>>& stmts) const
	{
		bool replaced = false;
		
		for(auto& s : stmts)
		{
			replaced |= replace(s);
		}
		
		return replaced;
	}
		
	void Replacer::visit(NoopStmt *)
	{
	}

	void Replacer::visit(Statements *original)
	{
		replace(original->stmts());
	}
	void Replacer::visit(IfStmt *original)
	{
		replace(original->condition());
		replace(original->then());
		replace(original->elseB());
	}
	void Replacer::visit(FuncCallStmt *original)
	{
		replace(original->arguments());
	}
	void Replacer::visit(AssignStmt *original)
	{
		replace(original->lhs());
		replace(original->rhs());
	}
	void Replacer::visit(ReturnStmt *original)
	{
		replace(original->_return());
	}
	void Replacer::visit(BreakStmt *)
	{
	}
	void Replacer::visit(ContinueStmt *)
	{
	}
	void Replacer::visit(SwitchStmt *original)
	{
		replace(original->expr());
		replace(original->branches());
	}
	void Replacer::visit(MFuncCallStmt *original)
	{
		replace(original->arguments());
		for (auto& b : original->branches_)
		{
			replace(b.returnExprs_);
			replace(b.stmt_);
		}
	}
	void Replacer::visit(MReturnStmt *original)
	{
		replace(original->returnTarget().returnExprs_);
	}
	void Replacer::visit(BlockStmt * original)
	{
		replace(original->body());
	}

	void Replacer::visit(const UnaryExpr* original)
	{
		if (exprMap_.count(original)) return;
		
		auto expr = original->expr();
		if (replace(expr))
		{
			auto cloned = f_.Unary(original->type(), original->op(), expr);
			exprMap_.emplace(original, cloned);
		}
	}

	void Replacer::visit(const BinaryExpr* original)
	{
		if (exprMap_.count(original)) return;

		auto lhs = original->left(); bool lreplaced = replace(lhs);
		auto rhs = original->right(); bool rreplaced = replace(rhs);

		if (lreplaced || rreplaced)
		{
			auto cloned = f_.Binary(original->type(), original->op(), lhs, rhs);
			exprMap_.emplace(original, cloned);			
		}
	}

	void Replacer::visit(const StructAccessExpr* original)
	{
		if (exprMap_.count(original)) return;
		
		auto expr = original->ptr();
		if (replace(expr))
		{
			// WRONG !!! - use factory
			auto cloned = make_intrusive<StructAccessExpr>(original->type(), expr, original->ordinal());
			exprMap_.emplace(original, cloned);
		}
	}

	void Replacer::visit(const PointerArithmExpr* original)
	{
		if (exprMap_.count(original)) return;

		auto lhs = original->left(); bool lreplaced = replace(lhs);
		auto rhs = original->right(); bool rreplaced = replace(rhs);

		if (lreplaced || rreplaced)
		{
			// WRONG !!! - use factory
			auto cloned = make_intrusive<PointerArithmExpr>(original->type(), original->op(), lhs, rhs);
			exprMap_.emplace(original, cloned);			
		}
	}

	 void Replacer::visit(const DereferenceExpr* original)
	 {
		if (exprMap_.count(original)) return;

		auto expr = original->ptr();
		if (replace(expr))
		{
			// WRONG !!! - use factory
			auto cloned = make_intrusive<DereferenceExpr>(original->type(), expr);
			exprMap_.emplace(original, cloned);
		}		
	 }

	 void Replacer::visit(const AddressOfExpr* original)
	 {
		if (exprMap_.count(original)) return;
		
		auto expr = original->expr();
		if (replace(expr))
		{
			// WRONG !!! - use factory
			auto cloned = make_intrusive<AddressOfExpr>(original->type(), expr);
			exprMap_.emplace(original, cloned);
		}		
	}

	 void Replacer::visit(const VarExpr*)
	 {
	 }

	 void Replacer::visit(const ConstExpr*)
	 {
	 }

	 void Replacer::visit(const BitCastExpr* original)
	 {
		if (exprMap_.count(original)) return;

		auto expr = original->expr();
		if (replace(expr))
		{
			auto cloned = f_.BitCast(expr, original->type());
			exprMap_.emplace(original, cloned);
		}		
	}

	void Replacer::visit(const FuncCallExpr* original)
	{
		if (exprMap_.count(original)) return;
		
		auto arguments = original->arguments();
		if (replace(arguments))
		{
			auto cloned = f_.FuncExpr(original->function(), std::move(arguments));
			
			exprMap_.insert({ original, cloned });			
		}
	}

	void Replacer::visit(const HoleExpr*)
	{
	}
	
	void Replacer::visit(const InlinedFuncExpr* original)
	{
		if (exprMap_.count(original)) return;
		
		auto returnExpr = original->returnExpr(); bool replaced = replace(returnExpr);
		auto body = original->body(); replaced |= replace(body);

		if (replaced)
		{
			auto cloned = f_.InlinedFunc(returnExpr, body);
			
			exprMap_.insert({ original, cloned });						
		}
	}
}