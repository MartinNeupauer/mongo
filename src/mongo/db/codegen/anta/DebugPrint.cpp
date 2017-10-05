#include "DebugPrint.h"
#include "Expr.h"
#include "Statement.h"
#include "Function.h"
#include "Scope.h"

namespace anta
{
	static const char* op2string(ExprOp op)
	{
		switch(op)
		{
		case op_logical_not: return "!";
		case op_bit_not: return "~";
		case op_unary_minus: return "-";

		case op_arit_plus: return "+";
		case op_arit_minus: return "-";
		case op_arit_mult: return "*";
		case op_arit_div: return "/";
		case op_arit_rem: return "%";

		case op_logical_and: return "&&";
		case op_logical_or: return "||";

		case op_bit_and: return "&";
		case op_bit_or: return "|";
		case op_bit_xor: return "^";

		case op_bit_leftshift: return "<<";
		case op_bit_rightshift: return ">>";

		case op_comp_eq: return "==";
		case op_comp_neq: return "!=";
		case op_comp_less: return "<";
		case op_comp_greater: return ">";
		case op_comp_less_eq: return "<=";
		case op_comp_greater_eq: return ">=";
		};
		throw std::logic_error("unknown operation");
	}
	void DebugPrinter::print(Function * fn)
	{
		auto& returns = fn->returns();
		if (returns.size())
		{
			for (auto& r : returns)
			{
				indent(); os_ << "{\"" << r.label_ << "\"";
				for (auto t : r.returnTypes_)
					os_ << ", " << t->name();
				os_ << "}\n";
			}
			indent();
		} 
		else
		{
			indent();
			os_ << (fn->retType() ? fn->retType()->name() : "void") << " ";
		}
		auto& params = fn->parameters();
		os_ << fn->name() << "(";
		for (unsigned i = 0; i < params.size(); ++i)
		{
			if (i != 0)
				os_ << ", ";
			os_ << params[i]->type()->name() << " ";
			os_ << params[i]->name();
		}
		os_ << ")\n";
		openbracket();
		if (fn->body_)
		{
			fn->body_->visit(*this);
		}
		else
		{
			indent(); os_ << "<builtin>\n";
		}
		closebracket();
	}
	void DebugPrinter::visit(NoopStmt *)
	{
	}
	void DebugPrinter::visit(Statements *stmt)
	{
		for (auto s : stmt->stmts())
			s->visit(*this);
	}
	void DebugPrinter::visit(IfStmt *stmt)
	{
		indent(); os_ << "if(";
		stmt->condition()->visit(*this);
		os_ << ")\n";

		openbracket();
		stmt->then()->visit(*this);
		closebracket();

		if (!stmt->elseB()->is_a<NoopStmt>())
		{
			indent(); os_ << "else\n";
			openbracket();
			stmt->elseB()->visit(*this);
			closebracket();
		}
	}
	void DebugPrinter::visit(FuncCallStmt *stmt)
	{
		indent(); os_ << stmt->function()->name() << "(";

		auto& args = stmt->arguments();
		for (unsigned i = 0; i < args.size(); ++i)
		{
			if (i != 0)
				os_ << ", ";
			args[i]->visit(*this);
		}
		os_ << ")\n";
	}
	void DebugPrinter::visit(AssignStmt *stmt)
	{
		indent();
		stmt->lhs()->visit(*this); os_ << "="; stmt->rhs()->visit(*this); os_ << "\n";
	}
	void DebugPrinter::visit(ReturnStmt *stmt)
	{
		indent(); os_ << "return";
		if (stmt->_return())
		{
			os_ << "("; stmt->_return()->visit(*this); os_ << ")\n";
		}
		else
		{
			os_ << "\n";
		}
	}
	void DebugPrinter::visit(BreakStmt *stmt)
	{
		indent(); os_ << "break(" << stmt->level() << ")\n";
	}
	void DebugPrinter::visit(ContinueStmt *stmt)
	{
		indent(); os_ << "continue\n";
	}
	void DebugPrinter::visit(SwitchStmt *stmt)
	{
		indent(); os_ << "switch("; stmt->expr()->visit(*this); os_ << ")\n";
		openbracket();
		unsigned counter = 0;
		for (auto& b : stmt->branches())
		{
			indent(); os_ << counter << ":\n";
			++indent_;
			b->visit(*this);
			--indent_;

			++counter;
		}
		closebracket();
	}
	void DebugPrinter::visit(MFuncCallStmt *stmt)
	{
		indent(); os_ << stmt->function()->name() << "(";
		auto& args = stmt->arguments();
		for (unsigned i = 0; i < args.size(); ++i)
		{
			if (i != 0)
				os_ << ", ";
			args[i]->visit(*this);
		}
		os_ << ")\n";

		openbracket();
		for (auto& b : stmt->branches())
		{
			indent(); os_ << "\"" << b.label_ << "\"";
			for (auto& r : b.returnExprs_)
			{
				os_ << ", ";
				r->visit(*this);
			}
			os_ << "\n";

			++indent_;
			b.stmt_->visit(*this);
			--indent_;
		}
		closebracket();
	}
	void DebugPrinter::visit(MReturnStmt *stmt)
	{
		indent(); os_ << "return(\"" << stmt->returnTarget().label_ << "\"";
		for (auto e : stmt->returnTarget().returnExprs_)
		{
			os_ << ", ";
			e->visit(*this);
		}
		os_ << ")\n";
	}
	void DebugPrinter::visit(BlockStmt *stmt)
	{
		indent(); os_ << (stmt->loop() ? "loop\n" : "block\n");
		openbracket();
		stmt->body()->visit(*this);
		closebracket();
	}
	void DebugPrinter::visit(const UnaryExpr *expr)
	{
		os_ << "(" << op2string(expr->op()); expr->expr()->visit(*this); os_ << ")";
	}
	void DebugPrinter::visit(const BinaryExpr *expr)
	{
		os_ << "("; expr->left()->visit(*this); os_ << op2string(expr->op()); expr->right()->visit(*this); os_ << ")";
	}
	void DebugPrinter::visit(const StructAccessExpr *expr)
	{
		expr->ptr()->visit(*this);
		if (expr->ptrType())
		{
			os_ << "->";
		}
		else
		{
			os_ << ".";
		}
		auto type = expr->structType();
		os_ << type->fieldName(expr->ordinal());
	}
	void DebugPrinter::visit(const PointerArithmExpr *expr)
	{
		os_ << "("; expr->left()->visit(*this); os_ << "+"; expr->right()->visit(*this); os_ << ")";
	}
	void DebugPrinter::visit(const DereferenceExpr *expr)
	{
		os_ << "*("; expr->ptr()->visit(*this); os_ << ")";
	}
	void DebugPrinter::visit(const AddressOfExpr *expr)
	{
		os_ << "&("; expr->expr()->visit(*this); os_ << ")";
	}
	void DebugPrinter::visit(const VarExpr *expr)
	{
		os_ << expr->var()->scope()->name() << "." << expr->var()->name() << "<" << expr->type()->name() << ">";
	}
	void DebugPrinter::visit(const ConstExpr *expr)
	{
		os_ << "CONSTANT";
	}
	void DebugPrinter::visit(const BitCastExpr *expr)
	{
		os_ << "cast<" << expr->type()->name() << ">("; expr->expr()->visit(*this); os_ << ")";
	}
	void DebugPrinter::visit(const FuncCallExpr *expr)
	{
		os_ << expr->function()->name() << "(";

		auto& args = expr->arguments();
		for (unsigned i = 0; i < args.size(); ++i)
		{
			if (i != 0)
				os_ << ", ";
			args[i]->visit(*this);
		}
		os_ << ")";
	}
	void DebugPrinter::visit(const HoleExpr *expr)
	{
		expr->placeholder()->visit(*this);
	}
	void DebugPrinter::visit(const InlinedFuncExpr *expr)
	{
		os_ << "["; expr->returnExpr()->visit(*this); os_ << "]\n";
		openbracket();
		expr->body()->visit(*this);
		closebracket();
	}
}