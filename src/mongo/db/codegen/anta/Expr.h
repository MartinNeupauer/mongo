#pragma once

#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/Value.h"
#include "mongo/db/codegen/anta/Variable.h"

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <stdexcept>
#include <vector>

namespace llvm
{
    class Value;
}

namespace anta
{
	using namespace boost;

	class Expr;

	enum class ExprCategory
	{
		unknown,
		lvalue,
		rvalue,
		nrvalue // freshly minted new rvalue - must be stored or freed othrwise we would leak
	};

	class ExprPlaceholders
	{
		std::vector<intrusive_ptr<const Expr>> placeholders_;
		std::vector<const anta::Type*> types_;
	public:
		ExprPlaceholders(const std::vector<const anta::Type*>& types) : types_(types) { placeholders_.resize(types_.size()); }

		auto& get(unsigned idx) const
		{
			return placeholders_[idx];
		}
		auto type(unsigned idx) const
		{
			return types_[idx];
		}

		ExprCategory category(unsigned idx) const;

		void fillHole(unsigned idx, intrusive_ptr<const Expr> expr);
		auto size() const { return types_.size(); }
		const auto& types() const { return types_; }
	};

    class Expr : public SemanticNode, public intrusive_ref_counter<Expr, thread_unsafe_counter>
    {
        const Type* type_;

		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const = 0;
		virtual llvm::Value* generateRefImpl(EnvCodeGenCtx& ectx) const
		{
			throw std::logic_error("illegal lval evaluation");
		}

	public:
        Expr(const Type* type) : type_(type)
		{
			if (!type)
				throw std::logic_error("type must not be null");
		}

        const Type* type() const
        {
            return type_;
        }

		const bool lvalue() const { return category() == ExprCategory::lvalue; }
		const bool rvalue() const { return category() == ExprCategory::rvalue || category() == ExprCategory::nrvalue; }

		virtual ExprCategory category() const = 0;

		llvm::Value* generateTopLevel(EnvCodeGenCtx& ectx) const;
		llvm::Value* generateRefTopLevel(EnvCodeGenCtx& ectx) const;
		
		llvm::Value* generate(EnvCodeGenCtx& ectx, bool move = true) const;
		llvm::Value* generateRef(EnvCodeGenCtx& ectx, bool move = true) const;

		virtual void visit(AbstractVisitor& v) const = 0;
		virtual void preorder(AbstractVisitor& v) const = 0;
		virtual void postorder(AbstractVisitor& v) const = 0;
	};

    enum ExprOp
    {
        op_logical_not,
        op_bit_not,
        op_unary_minus,

        op_arit_plus,
        op_arit_minus,
        op_arit_mult,
        op_arit_div,
        op_arit_rem,

        op_logical_and,
        op_logical_or,
        
        op_bit_and,
        op_bit_or,
        op_bit_xor,

        op_bit_leftshift,
        op_bit_rightshift,

        op_comp_eq,
        op_comp_neq,
        op_comp_less,
        op_comp_greater,
        op_comp_less_eq,
        op_comp_greater_eq,
    };

    class UnaryExpr : public Expr
    {
        const ExprOp   op_;
        const intrusive_ptr<const Expr>    expr_;
    public:
        UnaryExpr(const Type* retType, const ExprOp op, const intrusive_ptr<const Expr>& expr);

		auto op() const { return op_; }
		auto& expr() const { return expr_; }

		virtual ExprCategory category() const override { return ExprCategory::nrvalue; }
		
		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
    };

    class BinaryExpr : public Expr
    {
        const ExprOp   op_;
		const intrusive_ptr<const Expr>    left_;
		const intrusive_ptr<const Expr>    right_;
        const bool     shortcut_;
    public:
        BinaryExpr(const Type* retType, const ExprOp op, const intrusive_ptr<const Expr>& left, const intrusive_ptr<const Expr>& right);

		auto op() const { return op_; }
		auto& left() const { return left_; }
		auto& right() const { return right_; }

		virtual ExprCategory category() const override { return ExprCategory::nrvalue; }

        virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
    };

	class StructAccessExpr : public Expr
	{
		const intrusive_ptr<const Expr>   ptr_;
		const int     ordinal_;
	public:
		StructAccessExpr(const Type* retType, const intrusive_ptr<const Expr>& ptr, int ordinal);

		auto ordinal() const { return ordinal_; }
		auto& ptr() const { return ptr_; }
		auto ptrType() const { return ptr_->type()->is_a<PointerType>(); }
		auto structType() const { return (ptrType() ? ptrType()->pointee() : ptr_->type())->is_a<StructType>(); }
		virtual ExprCategory category() const override;

		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;
		virtual llvm::Value* generateRefImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};

	class PointerArithmExpr : public Expr
	{
		const ExprOp   op_;
		const intrusive_ptr<const Expr>    left_;
		const intrusive_ptr<const Expr>    right_;

	public:
		PointerArithmExpr(const Type* retType, const ExprOp op, const intrusive_ptr<const Expr>& left, const intrusive_ptr<const Expr>& right);

		auto op() const { return op_; }
		auto& left() const { return left_; }
		auto& right() const { return right_; }

		virtual ExprCategory category() const override { return ExprCategory::rvalue; }

		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};
	
	class DereferenceExpr : public Expr
	{
		const intrusive_ptr<const Expr>    ptr_;
	public:
		DereferenceExpr(const Type* retType, const intrusive_ptr<const Expr>& ptr);

		auto& ptr() const { return ptr_; }

		virtual ExprCategory category() const override { return ExprCategory::lvalue; }

		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;
		virtual llvm::Value* generateRefImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};

	class AddressOfExpr : public Expr
	{
		const intrusive_ptr<const Expr>    expr_;
	public:
		AddressOfExpr(const Type* retType, const intrusive_ptr<const Expr>& ptr);

		auto& expr() const { return expr_; }

		virtual ExprCategory category() const override { return ExprCategory::rvalue; }
		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};

	class VarExpr : public Expr
    {
        const Variable* variable_;
    public:
        VarExpr(const Variable* v) : Expr(v->type()), variable_(v) {}

		virtual ExprCategory category() const override { return variable_->isssa() ? ExprCategory::rvalue : ExprCategory::lvalue; }
		
		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;
        virtual llvm::Value* generateRefImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;

		bool isssa() const { return variable_->isssa(); }
		auto var() const { return variable_; }
    };

    class ConstExpr : public Expr
    {
        const Value  constant_;

    public:
        ConstExpr(const Value& v) : Expr(v.type()), constant_(v) {}

		const Value&         value() const { return constant_; }

		virtual ExprCategory category() const override { return ExprCategory::rvalue; }
		
		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};

	class BitCastExpr : public Expr
	{
		const intrusive_ptr<const Expr> expr_;
	public:
		BitCastExpr(const Type* retType, const intrusive_ptr<const Expr>& expr);

		auto& expr() const { return expr_; }
		bool delinearize() const;

		virtual ExprCategory category() const override { return ExprCategory::rvalue; }

		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};

    class FuncCallExpr : public Expr
    {
        const Function*                 function_;
        const std::vector<intrusive_ptr<const Expr>>  arguments_;
    public:
        FuncCallExpr(const Function* function, std::vector<intrusive_ptr<const Expr>> args);

		auto function() const { return function_; }
		auto& arguments() const { return arguments_; }

		virtual ExprCategory category() const override { return ExprCategory::nrvalue; }
		
		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override; 
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};

	class HoleExpr : public Expr
	{
		const ExprPlaceholders* env_;
		unsigned slot_;
	public:
		HoleExpr(const ExprPlaceholders* env, unsigned slot);

		auto env() const { return env_; }
		auto slot() const { return slot_; }

		auto& placeholder() const { return env_->get(slot_); }

		virtual ExprCategory category() const override { return env_->category(slot_); }

		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;
		virtual llvm::Value* generateRefImpl(EnvCodeGenCtx& ectx) const override;

		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
	};

	class InlinedFuncExpr : public Expr
	{
		const intrusive_ptr<const Expr> returnExpr_;
		const intrusive_ptr<Statement> body_;
	public:
		InlinedFuncExpr(const intrusive_ptr<const Expr>& returnExpr, const intrusive_ptr<Statement>& body) : Expr(returnExpr->type()), returnExpr_(returnExpr), body_(body) {}

		auto& returnExpr() const { return returnExpr_;}
		auto& body() const { return body_; }

		virtual ExprCategory category() const override { return ExprCategory::nrvalue; }

		virtual llvm::Value* generateImpl(EnvCodeGenCtx& ectx) const override;
		
		virtual void visit(AbstractVisitor& v) const override;
		virtual void preorder(AbstractVisitor& v) const override;
		virtual void postorder(AbstractVisitor& v) const override;
		};
}