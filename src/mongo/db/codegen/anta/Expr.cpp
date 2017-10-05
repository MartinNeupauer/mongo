#include "mongo/db/codegen/anta/EnvCodeGenCtx.h"
#include "mongo/db/codegen/anta/Expr.h"
#include "mongo/db/codegen/anta/Factory.h"
#include "mongo/db/codegen/anta/Visitor.h"

#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Module.h>

namespace anta
{
	llvm::Value * Expr::generateTopLevel(EnvCodeGenCtx & ectx) const
	{
		llvm::Value* value = generate(ectx);

		for (auto& v : ectx.exprKillSet_)
			v.type_->generateKill(ectx, v.value_);

		ectx.exprKillSet_.clear();

		return value;
	}

	llvm::Value * Expr::generateRefTopLevel(EnvCodeGenCtx & ectx) const
	{
		llvm::Value* value = generateRef(ectx);

		for (auto& v : ectx.exprKillSet_)
			v.type_->generateKill(ectx, v.value_);

		ectx.exprKillSet_.clear();

		return value;
	}

	llvm::Value * Expr::generate(EnvCodeGenCtx & ectx, bool move) const
	{
		llvm::Value* value;

		auto var = is_a<VarExpr>() ? is_a<VarExpr>()->var() : nullptr;

		// check the variable is defined
		if (var && !ectx.isDefined(var))
			throw std::logic_error("accessing uninitialized variable");

		if (type()->linear() && move)
		{
			if (lvalue())
			{
				auto ptr = generateRefImpl(ectx);
				value = ectx.builder_.CreateLoad(ptr);

				// unless we can prove that this is the last usage of rhs we must undef it
				type()->generateUndef(ectx, ptr);

			}
			else if (rvalue())
			{
				// how do we prevent multiple use when we cannot undef rvalue?
				value = generateImpl(ectx);
			}
			else
				throw std::logic_error("unknown expression category");

		}
		else
		{
			value = generateImpl(ectx);
		}

		if (type()->linear())
		{
			// unless we can prove that the value is valid we must check
			type()->generateCheckUndef(ectx, value);

			// record that the variable has been used and cannot be used until redefined
			if (var && move)
				ectx.definedVariables_[var] = used;
		}

		// add a value to a list of values to be killed when the expression is fully evaluated
		if (type()->linear() && category() == ExprCategory::nrvalue && !move)
		{
			ectx.exprKillSet_.push_back({ value,type() });
		}

		return value;
	}

	llvm::Value * Expr::generateRef(EnvCodeGenCtx & ectx, bool move) const
	{
		if (!lvalue())
			throw std::logic_error("cannot generate reference to rvalue");

		llvm::Value* ptr = generateRefImpl(ectx);

		auto var = is_a<VarExpr>() ? is_a<VarExpr>()->var() : nullptr;

		if (type()->linear() && move)
		{
			// unless we can prove that lhs is dead we must kill the previous value in lhs!
			if (!var || ectx.isDefined(var) || ectx.isDivergent(var))
			{
				auto value = ectx.builder_.CreateLoad(ptr);
				type()->generateKill(ectx, value);
			}
		}

		if (var)
		{
			if (move)
			{
				// this is a (re)definition of a variable, record it
				ectx.definedVariables_[var] = defined;
				// also remove any metion of used variable here (for linear types)
			}
			else
			{
				// check the variable is defined
				if (!ectx.isDefined(var))
					throw std::logic_error("accessing uninitialized variable");
			}
		}
		return ptr;
	}

    UnaryExpr::UnaryExpr(const Type* retType, const ExprOp op, const intrusive_ptr<const Expr>& expr) : Expr(retType)
        , op_(op)
        , expr_(expr)
    {
        switch (op)
        {
        case op_logical_not:
            break;
        case op_bit_not:
            break;
        case op_unary_minus:
            break;
        default:
            throw std::runtime_error("unknown unary operator");
        }
    }

    llvm::Value* UnaryExpr::generateImpl(EnvCodeGenCtx& ectx) const
    {
        llvm::Value* val = expr_->generate(ectx);
        llvm::Value* result = nullptr;

        switch (op_)
        {
        case op_logical_not:
			result = ectx.builder_.CreateNot(val);
			break;
        case op_bit_not:
            // value ^ allones
            result = ectx.builder_.CreateXor(llvm::Constant::getAllOnesValue(val->getType()), val);
            break;
        case op_unary_minus:
            // 0 - value
            result = ectx.builder_.CreateSub(llvm::Constant::getNullValue(val->getType()), val);
            break;
        default:
            throw std::logic_error("unknown unary operator");
        }
        return result;
    }

	void UnaryExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void UnaryExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		expr_->preorder(v);
	}

	void UnaryExpr::postorder(AbstractVisitor & v) const
	{
		expr_->postorder(v);

		v.visit(this);
	}

    BinaryExpr::BinaryExpr(const Type* retType, const ExprOp op, const intrusive_ptr<const Expr>& left, const intrusive_ptr<const Expr>& right) : Expr(retType)
        , op_(op)
        , left_(left)
        , right_(right)
        , shortcut_(op == op_logical_and || op == op_logical_or)
    {
    }

    llvm::Value* BinaryExpr::generateImpl(EnvCodeGenCtx& ectx) const
    {
        llvm::Value* result = nullptr;
        llvm::Value* left = left_->generate(ectx);
        llvm::Value* right = nullptr;

        if (!shortcut_)
            right = right_->generate(ectx);

        switch (op_)
        {
        case op_arit_plus:
            if      (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateAdd(left, right); }
            else if (left->getType()->isDoubleTy())  { result = ectx.builder_.CreateFAdd(left, right); }
            break;
        case op_arit_minus:
            if      (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateSub(left, right); }
            else if (left->getType()->isDoubleTy())  { result = ectx.builder_.CreateFSub(left, right); }
            break;
        case op_arit_mult:
            if      (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateMul(left, right); }
            else if (left->getType()->isDoubleTy())  { result = ectx.builder_.CreateFMul(left, right); }
            break;
        case op_arit_div:
            if      (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateSDiv(left, right); }
            else if (left->getType()->isDoubleTy())  { result = ectx.builder_.CreateFDiv(left, right); }
            break;
        case op_arit_rem:
            if      (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateSRem(left, right); }
            else if (left->getType()->isDoubleTy())  { result = ectx.builder_.CreateFRem(left, right); }
            break;

		case op_bit_and:
			if (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateAnd(left, right); }
			break;
		case op_bit_or:
			if (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateOr(left, right); }
			break;
		case op_bit_xor:
			if (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateXor(left, right); }
			break;
		case op_bit_leftshift:
			if (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateShl(left, right); }
			break;
		case op_bit_rightshift:
			if (left->getType()->isIntegerTy()) { result = ectx.builder_.CreateLShr(left, right); }
			break;

		case op_comp_eq:
			if (left->getType()->isIntegerTy() || left->getType()->isPointerTy()) { result = ectx.builder_.CreateICmpEQ(left, right); }
			else if (left->getType()->isDoubleTy()) { result = ectx.builder_.CreateFCmpOEQ(left, right); }
			break;
		case op_comp_neq:
			if (left->getType()->isIntegerTy() || left->getType()->isPointerTy()) { result = ectx.builder_.CreateICmpNE(left, right); }
			else if (left->getType()->isDoubleTy()) { result = ectx.builder_.CreateFCmpONE(left, right); }
			break;
		case op_comp_less:
			if (left->getType()->isIntegerTy() || left->getType()->isPointerTy()) { result = ectx.builder_.CreateICmpSLT(left, right); }
			else if (left->getType()->isDoubleTy()) { result = ectx.builder_.CreateFCmpOLT(left, right); }
			break;
		case op_comp_greater:
			if (left->getType()->isIntegerTy() || left->getType()->isPointerTy()) { result = ectx.builder_.CreateICmpSGT(left, right); }
			else if (left->getType()->isDoubleTy()) { result = ectx.builder_.CreateFCmpOGT(left, right); }
			break;
		case op_comp_less_eq:
			if (left->getType()->isIntegerTy() || left->getType()->isPointerTy()) { result = ectx.builder_.CreateICmpSLE(left, right); }
			else if (left->getType()->isDoubleTy()) { result = ectx.builder_.CreateFCmpOLE(left, right); }
			break;
		case op_comp_greater_eq:
			if (left->getType()->isIntegerTy() || left->getType()->isPointerTy()) { result = ectx.builder_.CreateICmpSGE(left, right); }
			else if (left->getType()->isDoubleTy()) { result = ectx.builder_.CreateFCmpOGE(left, right); }
			break;
		default:
			throw std::logic_error("unknown operator");
		}

		switch (op_)
		{
		case op_comp_eq:
		case op_comp_neq:
		case op_comp_less:
		case op_comp_greater:
		case op_comp_less_eq:
		case op_comp_greater_eq:
			result = ectx.builder_.CreateZExt(result, llvm::Type::getInt32Ty(ectx.ctx_.context_));
		default:
			;
		}
		return result;
	}

	void BinaryExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void BinaryExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		left_->preorder(v);
		right_->preorder(v);
	}

	void BinaryExpr::postorder(AbstractVisitor & v) const
	{
		left_->postorder(v);
		right_->postorder(v);

		v.visit(this);
	}

	llvm::Value* VarExpr::generateImpl(EnvCodeGenCtx& ectx) const
	{
		llvm::Value* var = ectx.getVariable(variable_);
		if (variable_->isssa())
		{
			return var;
		}

		return ectx.builder_.CreateLoad(var);
	}

	llvm::Value* VarExpr::generateRefImpl(EnvCodeGenCtx& ectx) const
	{
		if (!lvalue())
			throw std::logic_error("cannot reference rvalue");

		return ectx.getVariable(variable_);
	}

	void VarExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void VarExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void VarExpr::postorder(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	llvm::Value* ConstExpr::generateImpl(EnvCodeGenCtx& ectx) const
	{
		auto ptr = constant_.type()->is_a<PointerType>();
		if (ptr && ptr->pointee()->is_a<FunctionType>())
		{
			Function* fn = constant_.value<Function*>();
			llvm::Function* llvmfn = fn->getllvmfn();
			return llvmfn;
		}
        llvm::Type* type = constant_.type()->getllvm(ectx.ctx_);

		if (!type)
			throw std::logic_error("invalid type in a constant");

        if (type->isIntegerTy())
        {
            return llvm::ConstantInt::get(type, constant_.value<int>(), true);
        }
        else if (type->isDoubleTy())
        {
            return llvm::ConstantFP::get(type, constant_.value<double>());
        }
		else if (constant_.hasString())
		{
			return ectx.builder_.CreateGlobalStringPtr(constant_.value<std::string>());
		}
		else if (type->isPointerTy())
		{
			return llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(type));
		}
        else
            throw std::logic_error("invalid type in a constant");
    }

	void ConstExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void ConstExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void ConstExpr::postorder(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	BitCastExpr::BitCastExpr(const Type* retType, const intrusive_ptr<const Expr>& expr) : Expr(retType), expr_(expr)
	{
	}

	bool BitCastExpr::delinearize() const
	{
		auto fromType = expr_->type();
		auto toType = type();

		if (typeid(fromType) != typeid(toType))
			return false;

		if (!(fromType->linear() && !toType->linear()))
			return false;

		return true;
	}

	llvm::Value* BitCastExpr::generateImpl(EnvCodeGenCtx& ectx) const
	{
		auto fromValue = expr_->generate(ectx, false);
		auto toType = type()->getllvm(ectx.ctx_);

		// if the underlying LLVM type is the same then we are done
		if (fromValue->getType() == toType)
		{
			return fromValue;
		}

		if (expr_->type()->isInteger() && type()->is_a<DoubleType>())
		{
			return ectx.builder_.CreateSIToFP(fromValue, toType);
		}
		return ectx.builder_.CreateBitCast(fromValue, toType);
	}

	void BitCastExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void BitCastExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		expr_->preorder(v);
	}

	void BitCastExpr::postorder(AbstractVisitor & v) const
	{
		expr_->postorder(v);

		v.visit(this);
	}

    FuncCallExpr::FuncCallExpr(const Function* function, std::vector<intrusive_ptr<const Expr>> args) 
		: Expr(function->retType())
		, function_(function)
		, arguments_(std::move(args))
    {
		if (function_->parameters().size() != arguments_.size())
			throw std::logic_error("arguments and parameters do not match");

		for (unsigned i = 0; i < arguments_.size(); ++i)
		{
			if (arguments_[i]->type() != function_->parameters()[i]->type())
				throw std::logic_error("argument and parameter types do not match");
		}
    }

    llvm::Value* FuncCallExpr::generateImpl(EnvCodeGenCtx& ectx) const
    {
		std::vector<llvm::Value*> args;

		for (const auto& e : arguments_)
		{
			args.push_back(e->generate(ectx));
		}
		
		return ectx.CreateCall(function_, args);
    }

	void FuncCallExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void FuncCallExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		for (auto& e : arguments_)
			e->preorder(v);
	}

	void FuncCallExpr::postorder(AbstractVisitor & v) const
	{
		for (auto& e : arguments_)
			e->postorder(v);

		v.visit(this);
	}

	StructAccessExpr::StructAccessExpr(const Type* retType, const intrusive_ptr<const Expr>& ptr, int ordinal) : Expr(retType), ptr_(ptr), ordinal_(ordinal)
	{

	}

	ExprCategory StructAccessExpr::category() const
	{
		auto ptrType = ptr_->type()->is_a<PointerType>();
		if (ptrType)
		{
			return ExprCategory::lvalue;
		}
		return ptr_->category();
	}

	llvm::Value* StructAccessExpr::generateImpl(EnvCodeGenCtx& ectx) const
	{
		if (ptr_->type()->is_a<PointerType>())
		{
			llvm::Value* ptr = generateRef(ectx, false);

			return ectx.builder_.CreateLoad(ptr);
		}
		else
		{
			llvm::Value* ptr = ptr_->generate(ectx, false);
			unsigned idx[1];
			idx[0] = ordinal_;

			return ectx.builder_.CreateExtractValue(ptr, idx);
		}
	}

	llvm::Value* StructAccessExpr::generateRefImpl(EnvCodeGenCtx& ectx) const
	{
		if (!lvalue())
			throw std::logic_error("cannot reference rvalue");

		const PointerType* ptrType = ptr_->type()->is_a<PointerType>();
		if (!ptrType)
		{
			llvm::Value* ptr = ptr_->generateRef(ectx, false);
			llvm::Value* idx[2];
			idx[0] = llvm::ConstantInt::get(llvm::Type::getInt32Ty(ectx.ctx_.context_), 0, true);
			idx[1] = llvm::ConstantInt::get(llvm::Type::getInt32Ty(ectx.ctx_.context_), ordinal_, true);

			return ectx.builder_.CreateInBoundsGEP(nullptr, ptr, idx);
		}

		llvm::Value* ptr = ptr_->generate(ectx, false);
		llvm::Value* idx[2];
		idx[0] = llvm::ConstantInt::get(llvm::Type::getInt32Ty(ectx.ctx_.context_), 0, true);
		idx[1] = llvm::ConstantInt::get(llvm::Type::getInt32Ty(ectx.ctx_.context_), ordinal_, true);

		return ectx.builder_.CreateInBoundsGEP(ptrType->pointee()->getllvm(ectx.ctx_), ptr, idx);
	}

	void StructAccessExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void StructAccessExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		ptr_->preorder(v);
	}

	void StructAccessExpr::postorder(AbstractVisitor & v) const
	{
		ptr_->postorder(v);

		v.visit(this);
	}

	PointerArithmExpr::PointerArithmExpr(const Type * retType, const ExprOp op, const intrusive_ptr<const Expr>& left, const intrusive_ptr<const Expr>& right) : Expr(retType), op_(op), left_(left), right_(right)
	{
		if (op != op_arit_plus)
			throw std::logic_error("only + is allowed in arithmetic operators");
	}

	llvm::Value * PointerArithmExpr::generateImpl(EnvCodeGenCtx & ectx) const
	{
		const PointerType* ptrType = left_->type()->is_a<PointerType>();
		if (!ptrType)
			throw std::logic_error("pointer arithmetics requires a pointer");

		llvm::Value* left = left_->generate(ectx, false);
		llvm::Value* right = right_->generate(ectx, false);

		return ectx.builder_.CreateInBoundsGEP(ptrType->pointee()->getllvm(ectx.ctx_), left, right);
	}

	void PointerArithmExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void PointerArithmExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		left_->preorder(v);
		right_->preorder(v);
	}

	void PointerArithmExpr::postorder(AbstractVisitor & v) const
	{
		left_->postorder(v);
		right_->postorder(v);

		v.visit(this);
	}

	DereferenceExpr::DereferenceExpr(const Type * retType, const intrusive_ptr<const Expr>& ptr) : Expr(retType), ptr_(ptr)
	{
	}

	llvm::Value * DereferenceExpr::generateImpl(EnvCodeGenCtx & ectx) const
	{
		llvm::Value* ptr = ptr_->generate(ectx);

		return ectx.builder_.CreateLoad(ptr);
	}

	llvm::Value * DereferenceExpr::generateRefImpl(EnvCodeGenCtx & ectx) const
	{
		if (!lvalue())
			throw std::logic_error("cannot reference rvalue");

		return ptr_->generate(ectx);
	}

	void DereferenceExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void DereferenceExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		ptr_->preorder(v);
	}

	void DereferenceExpr::postorder(AbstractVisitor & v) const
	{
		ptr_->postorder(v);

		v.visit(this);
	}

	AddressOfExpr::AddressOfExpr(const Type * retType, const intrusive_ptr<const Expr>& ptr) : Expr(retType), expr_(ptr)
	{
	}

	llvm::Value * AddressOfExpr::generateImpl(EnvCodeGenCtx & ectx) const
	{
		return expr_->generateRef(ectx, false);
	}

	void AddressOfExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void AddressOfExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		expr_->preorder(v);
	}

	void AddressOfExpr::postorder(AbstractVisitor & v) const
	{
		expr_->postorder(v);

		v.visit(this);
	}

	HoleExpr::HoleExpr(const ExprPlaceholders * env, unsigned slot)
		: Expr(env->types()[slot])
		, env_(env)
		, slot_(slot)
	{
	}

	llvm::Value * HoleExpr::generateImpl(EnvCodeGenCtx & ectx) const
	{
		if (type() != placeholder()->type())
			throw std::logic_error("placeholder and hole must have the same type");

		return placeholder()->generate(ectx);
	}

	llvm::Value * HoleExpr::generateRefImpl(EnvCodeGenCtx & ectx) const
	{
		if (type() != placeholder()->type())
			throw std::logic_error("placeholder and hole must have the same type");

		return placeholder()->generateRef(ectx);
	}

	void HoleExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void HoleExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		placeholder()->preorder(v);
	}

	void HoleExpr::postorder(AbstractVisitor & v) const
	{
		placeholder()->postorder(v);

		v.visit(this);
	}

	ExprCategory ExprPlaceholders::category(unsigned idx) const
	{
		return get(idx) ? get(idx)->category() : ExprCategory::unknown;
	}

	void ExprPlaceholders::fillHole(unsigned idx, intrusive_ptr<const Expr> expr)
	{
		if (placeholders_[idx])
			throw std::logic_error("the hole is already filled");

		if (types_[idx] != expr->type())
			throw std::logic_error("the hole and expression have incompatible types");
	
		placeholders_[idx] = expr;
	}

	llvm::Value* InlinedFuncExpr::generateImpl(EnvCodeGenCtx& ectx) const
	{
		body_->generate(ectx);

		auto ret = returnExpr_->generate(ectx);

		return ret;
	}

	void InlinedFuncExpr::visit(AbstractVisitor & v) const
	{
		v.visit(this);
	}

	void InlinedFuncExpr::preorder(AbstractVisitor & v) const
	{
		v.visit(this);

		returnExpr()->preorder(v);
		body()->preorder(v);
	}

	void InlinedFuncExpr::postorder(AbstractVisitor & v) const
	{
		returnExpr()->postorder(v);
		body()->postorder(v);

		v.visit(this);
	}
}