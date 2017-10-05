#define __STDC_FORMAT_MACROS 1

#include <inttypes.h>

#include "Factory.h"
#include "CodeGenCtx.h"
#include "LowerMFunc.h"
#include "LowerForLoop.h"
#include "InlineFunction.h"
#include "Analysis.h"
#include <algorithm>
#include <iostream>

// hack hack
unsigned gblStrAllocs = 0;
unsigned gblStrFrees = 0;

namespace
{
	void killString(char* i)
	{
		free(i);
		++gblStrFrees;
	}

	void printInt(const int i)
	{
		printf("%d ", i);
	}

	void printInt64(const int64_t i)
	{
		printf("%" PRId64 " ", i);
	}

	void printPointer(const char* i)
	{
		printf("%p ", i);
	}

	void printDouble(const double d)
	{
		printf("%f ", d);
	}

	void printString(const char* i)
	{
		//const std::string* s = reinterpret_cast<const std::string*>(i);
		//printf("%s\n", s->c_str());
		printf("%s ", i);
	}

	void printLn()
	{
		printf("\n");
	}
}

namespace anta
{
    std::string SemaFactory::generateUniqueName(const std::string& name)
    {
		return name + std::to_string(nameCounter_++);
    }

    SemaFactory::SemaFactory()
        : nameCounter_(0)
        , globalScope_(nullptr)
    {
        createScope("__global", nullptr);

		globalScope_->addType("int1", std::make_unique<Int1Type>(globalScope()));
		globalScope_->addType("int", std::make_unique<IntType>(globalScope()));
		globalScope_->addType("int8", std::make_unique<Int8Type>(globalScope()));
		globalScope_->addType("int16", std::make_unique<Int16Type>(globalScope()));
		globalScope_->addType("int64", std::make_unique<Int64Type>(globalScope()));
		globalScope_->addType("double", std::make_unique<DoubleType>(globalScope()));
        globalScope_->addType("string", std::make_unique<StringType>(globalScope(), UInt8Type(), true));
		globalScope_->addType("weak_string", std::make_unique<StringType>(globalScope(), UInt8Type(), false));

		Function* f = createFunction(globalScope(), "printInt", (void*)::printInt); f->setConst();
        f->setParameters({ createVariable(f->paramScope(), UIntType(), "i") });

		Function* fprintPointer = createFunction(globalScope(), "printPointer", (void*)::printPointer); fprintPointer->setConst();
		fprintPointer->setParameters({ createVariable(fprintPointer->paramScope(), UPtrToInt8Type(), "i") });

		Function* fprintString = createFunction(globalScope(), "printString", (void*)::printString); fprintString->setConst();
		fprintString->setParameters({ createVariable(fprintString->paramScope(), UStringType(true), "i") });

		Function* fprintDouble = createFunction(globalScope(), "printDouble", (void*)::printDouble); fprintDouble->setConst();
		fprintDouble->setParameters({ createVariable(fprintDouble->paramScope(), UDoubleType(), "d") });

		{
			Function* f = createFunction(globalScope(), "printInt64", (void*)::printInt64); f->setConst();
			f->setParameters({ createVariable(f->paramScope(), UInt64Type(), "i") });
		}

		{
			Function* f = createFunction(globalScope(), "printLn", (void*)::printLn); f->setConst();
		}

		Function* fkillstring = createFunction(globalScope(), "killString", (void*)::killString);
		fkillstring->setParameters({ createVariable(fkillstring->paramScope(), UStringType(), "i") });

		auto retStruct = UStructType(globalScope(), "");
		retStruct->addField(UIntType(), "result");
		retStruct->addField(UInt1Type(), "overflow");

		Function* fadd = createFunction(globalScope(), retStruct, "llvm.sadd.with.overflow.i32");
		fadd->setParameters({ createVariable(fadd->paramScope(), UIntType(), "lhs"), createVariable(fadd->paramScope(), UIntType(), "rhs") });
		{
			createFunction(globalScope(), "llvm.trap");
		}
    }

    Scope* SemaFactory::createScope(const std::string& name, Scope* parent)
    {
		std::unique_ptr<Scope> scope = std::make_unique<Scope>(name, parent);
		Scope* ret = scope.get();
		if (parent)
		{
			parent->addChild(std::move(scope));
		}
		else
		{
			if (globalScope_)
				throw std::logic_error("global scope already created");
			globalScope_ = std::move(scope);
		}
		return ret;
    }

    Scope* SemaFactory::createScope(Scope* parent, const std::string& name)
    {
        if (!parent)
            throw std::logic_error("parent scope cannot be nullptr");

        return createScope(generateUniqueName(name.empty() ? "scope" : name), parent);
    }

    Variable* SemaFactory::createVariable(Scope* scope, const Type* type, std::string name, bool ssa)
    {
		return scope->addVariable(name, std::make_unique<Variable>(scope, type, name, ssa));
    }

	intrusive_ptr<const Expr> SemaFactory::UFuncPtr(Function* f)
	{
		auto it = funptrs_.find(f);

		if (it == funptrs_.end())
		{
			intrusive_ptr<const Expr> c = make_intrusive<const ConstExpr>(Value(UPtrToType(f->type()),f));
			it = funptrs_.insert({ f, c }).first;
		}
		return it->second.get();
	}

	intrusive_ptr<const Expr> SemaFactory::UNullPtr(const Type* t)
	{
		auto it = nullptrs_.find(t);

		if (it == nullptrs_.end())
		{
			intrusive_ptr<const Expr> c = make_intrusive<const ConstExpr>(Value(t));
			it = nullptrs_.insert({ t, c }).first;
		}
		return it->second.get();
	}

	intrusive_ptr<const Expr> SemaFactory::UIntConst(const int i)
    {
		auto it = intConstants_.find(i);

		if (it == intConstants_.end())
		{
			intrusive_ptr<const Expr> c = make_intrusive<const ConstExpr>(Value(UIntType(), i));
			it = intConstants_.insert({ i, c }).first;
		}
		return it->second.get();
    }

	intrusive_ptr<const Expr> SemaFactory::UDoubleConst(const double d)
	{
		auto it = doubleConstants_.find(d);

		if (it == doubleConstants_.end())
		{
			intrusive_ptr<const Expr> c = make_intrusive<const ConstExpr>(Value(UDoubleType(), d));
			it = doubleConstants_.insert({ d, c }).first;
		}
		return it->second.get();
	}
	
	intrusive_ptr<const Expr> SemaFactory::CastInteger(const intrusive_ptr<const Expr>& e, const Type* t)
	{
		auto ce = e->is_a<ConstExpr>();
		if (!ce)
			throw std::logic_error("cast works only on constants");

		return make_intrusive<ConstExpr>(Value(t, ce->value().value<int>()));
	}

	intrusive_ptr<const Expr> SemaFactory::BitCast(const intrusive_ptr<const Expr>& e, const Type* t)
	{
		if (e->is_a<ConstExpr>() && e->type()->isInteger())
			return CastInteger(e, t);

		if (!e->type()->canCast(t))
			throw std::logic_error("invalid cast");

		return make_intrusive<BitCastExpr>(t,e);
	}

	intrusive_ptr<const Expr> SemaFactory::UStringConst(const std::string& s)
	{
		auto it = stringConstants_.find(s);

		if (it == stringConstants_.end())
		{
			intrusive_ptr<const Expr> c = make_intrusive<const ConstExpr>(Value(UStringType(true), s));
			it = stringConstants_.insert({ s, c }).first;
		}
		return it->second.get();
	}

	intrusive_ptr<const Expr> SemaFactory::Var(Variable* v)
    {
		return make_intrusive<VarExpr>(v);
    }

	intrusive_ptr<const Expr> SemaFactory::Cmp(ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs)
    {
        switch (op)
        {
        case op_comp_eq:
        case op_comp_neq:
        case op_comp_less:
        case op_comp_greater:
        case op_comp_less_eq:
        case op_comp_greater_eq:
            if (lhs->type() != rhs->type() || !(lhs->type()->isInteger() || lhs->type() == UDoubleType() || lhs->type()->is_a<PointerType>()))
                throw std::logic_error("incompatible types in a binary comparison expression");
            break;
        default:
            throw std::runtime_error("unknown binary comparison operator");
        }
		return make_intrusive<BinaryExpr>(UIntType(), op, lhs, rhs);
    }

	intrusive_ptr<const Expr> SemaFactory::Unary(const Type* t, ExprOp op, const intrusive_ptr<const Expr>& expr)
	{
		return make_intrusive<UnaryExpr>(t, op, expr);
	}

	intrusive_ptr<const Expr> SemaFactory::Binary(const Type* t, ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs)
	{
		return make_intrusive<BinaryExpr>(t, op, lhs, rhs);
	}
	
	intrusive_ptr<const Expr> SemaFactory::Logic(ExprOp op, const intrusive_ptr<const Expr>& expr)
	{
		switch (op)
		{
		case op_logical_not:
			if (!(expr->type()->isInteger() || expr->is_a<PointerType>()))
				throw std::logic_error("incompatible types in an unary logical expression");
			break;
		default:
			throw std::runtime_error("unknown unary logical operator");
		}
		return make_intrusive<UnaryExpr>(expr->type(), op, expr);
	}

	intrusive_ptr<const Expr> SemaFactory::Arith(ExprOp op, const intrusive_ptr<const Expr>& expr)
	{
		switch (op)
		{
		case op_unary_minus:
			if (!(expr->type()->isInteger() || expr->type() == UDoubleType()))
				throw std::logic_error("incompatible types in an unary arithmetic expression");
			break;
		default:
			throw std::runtime_error("unknown unary arithmetic operator");
		}
		return make_intrusive<UnaryExpr>(expr->type(), op, expr);
	}

	intrusive_ptr<const Expr> SemaFactory::Arith(ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs)
    {
        switch (op)
        {
        case op_arit_plus:
			if (lhs->type()->is_a<PointerType>() && rhs->type()->isInteger())
			{
				return make_intrusive<PointerArithmExpr>(lhs->type(), op, lhs, rhs);
			}
        case op_arit_minus:
        case op_arit_mult:
        case op_arit_div:
        case op_arit_rem:
            if (lhs->type() != rhs->type() || !(lhs->type()->isInteger() || lhs->type() == UDoubleType()))
                throw std::logic_error("incompatible types in a binary arithmetic expression");
            break;
        default:
            throw std::runtime_error("unknown binary arithmetic operator");
        }
		return make_intrusive<BinaryExpr>(lhs->type(), op, lhs, rhs);
    }

	intrusive_ptr<const Expr> SemaFactory::BitOp(ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs)
	{
		switch (op)
		{
		case op_bit_and:
		case op_bit_or:
		case op_bit_xor:
		case op_bit_leftshift:
		case op_bit_rightshift:
			if (lhs->type() != rhs->type() || !(lhs->type()->isInteger()))
				throw std::logic_error("incompatible types in a bit operation expression");
			break;
		default:
			throw std::runtime_error("unknown bit operator");
		}
		return make_intrusive<BinaryExpr>(lhs->type(), op, lhs, rhs);
	}

	intrusive_ptr<const Expr> SemaFactory::FuncExpr(const Function* fn, std::vector<intrusive_ptr<const Expr>>&& args)
    {
		return make_intrusive<FuncCallExpr>(fn, addCastIfNeeded(fn, std::move(args), 0));
    }

	const anta::StructType* SemaFactory::checkStructType(const boost::intrusive_ptr<const anta::Expr> & ptr)
	{
		const StructType* structType;

		// we can access a field both buy a direct reference (i.e. a.b) or by a pointer (i.e. a->b)
		auto ptrType = ptr->type()->is_a<PointerType>();
		if (ptrType)
		{
			structType = ptrType->pointee()->is_a<StructType>();
		}
		else
		{
			structType = ptr->type()->is_a<StructType>();
		}

		if (!structType)
		{
			throw std::logic_error("cannot access a field in non struct type");
		}

		return structType;
	}

	intrusive_ptr<const Expr> SemaFactory::addCastIfNeeded(const intrusive_ptr<const Expr>& expr, const Type * desiredType)
	{
		auto fromType = expr->type();
		if (fromType == desiredType)
			return expr;

		if (delinearize(fromType) == desiredType)
		{
			return BitCast(expr, desiredType);
		}

		throw std::logic_error("types do not match");
	}

	std::vector<intrusive_ptr<const Expr>>&& SemaFactory::addCastIfNeeded(const Function * fn, std::vector<intrusive_ptr<const Expr>>&& args, int offset)
	{
		if (args.size() + offset != fn->parameters().size())
			throw std::logic_error("arguments and parameters do not match");

		for (unsigned i = 0; i < args.size(); ++i)
		{
			args[i] = addCastIfNeeded(args[i], fn->parameters()[i + offset]->type());
		}

		return std::move(args);
	}

	intrusive_ptr<const Expr> SemaFactory::StructAccess(const intrusive_ptr<const Expr>& ptr, const std::string& field)
	{
		auto structType = checkStructType(ptr);

		int ordinal = structType->ordinal(field);
		
		return make_intrusive<StructAccessExpr>(structType->fieldType(ordinal), ptr, ordinal);
	}

	intrusive_ptr<const Expr> SemaFactory::StructAccess(const intrusive_ptr<const Expr>& ptr, int ordinal)
	{
		auto structType = checkStructType(ptr);

		return make_intrusive<StructAccessExpr>(structType->fieldType(ordinal), ptr, ordinal);
	}

	intrusive_ptr<const Expr> SemaFactory::Hole(const ExprPlaceholders * env, unsigned slot)
	{
		return make_intrusive<HoleExpr>(env, slot);
	}

	intrusive_ptr<const Expr> SemaFactory::InlinedFunc(const intrusive_ptr<const Expr>& returnExpr, const intrusive_ptr<Statement>& body)
	{
		return make_intrusive<InlinedFuncExpr>(returnExpr, body);
	}

	intrusive_ptr<const Expr> SemaFactory::Dereference(const intrusive_ptr<const Expr>& ptr)
	{
		auto ptrType = ptr->type()->is_a<PointerType>();
		if (!ptrType)
		{
			throw std::logic_error("cannot dereference non pointer type");
		}
		return make_intrusive<DereferenceExpr>(ptrType->pointee(), ptr);
	}

	intrusive_ptr<const Expr> SemaFactory::AddressOf(const intrusive_ptr<const Expr>& ptr)
	{
		if (!ptr->lvalue())
			throw std::logic_error("cannot take an address of value that is not lvalue");

		return make_intrusive<AddressOfExpr>(UPtrToType(ptr->type()), ptr);
	}

	intrusive_ptr<Statement> SemaFactory::Assign(const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs, bool initial)
    {
		auto castedRhs = addCastIfNeeded(rhs, lhs->type());

		// if we don't know the kind we will allow the assignment and check later
		if (lhs->category() != ExprCategory::unknown)
		{
			if (initial)
			{
				// the only place when it is allowed to assign to rvalue (initialize a ssa variable)
				auto var = lhs->is_a<VarExpr>();
				if (!lhs->lvalue() && !(var && var->isssa()))
					throw std::logic_error("cannot assign to lhs that is not lvalue");
			}
			else
			{
				if (!lhs->lvalue())
					throw std::logic_error("cannot assign to lhs that is not lvalue");
			}
		}

		return make_intrusive<AssignStmt>(lhs, castedRhs);
    }

	intrusive_ptr<Statement> SemaFactory::For(const intrusive_ptr<Statement>& init, const intrusive_ptr<const Expr>& condition, const intrusive_ptr<Statement>& step, const intrusive_ptr<Statement>& body)
    {
		LowerForLoop lower(*this);
		return lower.run(init, condition, step, body);
    }

	intrusive_ptr<Statement> SemaFactory::If(const intrusive_ptr<const Expr>& cond, const intrusive_ptr<Statement>& then, const intrusive_ptr<Statement>& else_)
    {
		return make_intrusive<IfStmt>(cond, then, else_);
    }

	intrusive_ptr<Statement> SemaFactory::FuncStmt(const Function* fn, std::vector<intrusive_ptr<const Expr>>&& args)
    {
		return make_intrusive<FuncCallStmt>(fn, addCastIfNeeded(fn, std::move(args), 0));
    }

	intrusive_ptr<Statement> SemaFactory::Return(const intrusive_ptr<const Expr>& ret)
    {
		return make_intrusive<ReturnStmt>(ret);
    }

	intrusive_ptr<Statement> SemaFactory::Switch(const intrusive_ptr<const Expr>& val, std::vector<intrusive_ptr<Statement>>&& branches)
	{
		return make_intrusive<SwitchStmt>(val, std::move(branches));
	}

	intrusive_ptr<Statement> SemaFactory::MFuncStmt(const Function * fn, std::vector<intrusive_ptr<const Expr>>&& args, std::vector<FuncletDef>&& branches)
	{
		return make_intrusive<MFuncCallStmt>(fn, addCastIfNeeded(fn, std::move(args), 1), std::move(branches));
	}

	intrusive_ptr<Statement> SemaFactory::MReturn(FuncletRet&& return_)
	{
		return make_intrusive<MReturnStmt>(std::move(return_));
	}

	intrusive_ptr<Statement> SemaFactory::Break(int level)
	{
		return make_intrusive<BreakStmt>(level);
	}

	intrusive_ptr<Statement> SemaFactory::Continue()
	{
		return make_intrusive<ContinueStmt>();
	}

	intrusive_ptr<Statement> SemaFactory::Block(const intrusive_ptr<Statement>& body, bool loop)
	{
		return make_intrusive<BlockStmt>(body, loop);
	}

	intrusive_ptr<Statement> SemaFactory::Noop()
	{
		return make_intrusive<NoopStmt>();
	}

	intrusive_ptr<Statement> SemaFactory::Stmts(std::vector<intrusive_ptr<Statement>>&& stmts)
    {
		if (stmts.empty()) return Noop();
		// commented out because SwitchStmt expects vectors (even with just 1 element)
		if (stmts.size() == 1) return stmts[0];

		return make_intrusive<Statements>(std::move(stmts));
    }

    void SemaFactory::generatePrototypes(CodeGenContext& ctx)
    {
        for (auto& f : functions_)
        {
            f->generateProto(ctx);
        }
    }

    void SemaFactory::generateFunctions(CodeGenContext& ctx)
    {
        for (auto& f : functions_)
        {
			f->generate(ctx);
        }
    }

	void SemaFactory::runOptimizations(CodeGenContext & ctx)
	{
		std::unordered_set<const anta::Function*> fnToInline;
		for (auto& f : functions_)
		{
			if (f->isInline())
				fnToInline.insert(f);
		}

		for (auto& f : functions_)
		{
			if (f->body_)
			{
				// order is important !!!

				LowerMFunc mfunclow(*this, *f);
				mfunclow.run();

				if (!fnToInline.empty())
				{
					InlineFunction inl(*this, *f, fnToInline);
					inl.run();
				}

				RemoveRedundantBlocks redundant(*this, *f);
				redundant.run();
			}
		}
	}

	void SemaFactory::generateStructTypes(CodeGenContext& ctx)
	{
		for (auto& s : structs_)
		{
			std::string tmp = s.first->getFullName();
			tmp.append(".");
			tmp.append(s.second->name());

			s.second->generateDeclaration(ctx, tmp);
		}

		for (auto& s : structs_)
		{
			s.second->generateDefinition(ctx);
		}
	}

	void SemaFactory::finalizeCodeGen(CodeGenContext & ctx)
	{
		runOptimizations(ctx);

		generateStructTypes(ctx);
		generatePrototypes(ctx);
		generateFunctions(ctx);
	}

	ExprPlaceholders * SemaFactory::createPlaceholders(const std::vector<const anta::Type*>& types)
	{
		placeholders_.emplace_back(std::make_unique<ExprPlaceholders>(types));
		return placeholders_.back().get();
	}

	void SemaFactory::destroyFunction(Function * f)
	{
		//auto scope = f->scope();
		functions_.erase( std::remove(functions_.begin(), functions_.end(), f), functions_.end());
	}

	Function* SemaFactory::createFunction(Scope* scope, const Type* ret, const std::string& name, void* builtin)
    {
        Scope* paramScope = createScope(generateUniqueName(name + "_params"), scope);

        Function* fn = scope->addFunction(name, std::make_unique<Function>(scope, ret, paramScope, name, builtin));
		functions_.push_back(fn);
        return fn;
    }

	Function* SemaFactory::createFunction(Scope* scope, const std::string& name, void* builtin)
	{
		return createFunction(scope, nullptr, name, builtin);
	}

	StructType* SemaFactory::UStructType(Scope* scope, const std::string& name)
	{
		std::string structname(name.empty() ? generateUniqueName("anonstruct") : name);

		StructType* ret = scope->addType(structname, std::make_unique<StructType>(scope, name))->is_a<StructType>();
		
		structs_.push_back({ scope, ret });
		return ret;
	}

	const Type* SemaFactory::delinearize(const Type* t)
	{
		if (!t->linear())
			return t;

		if (t->is_a<StringType>())
			return UStringType(true);

		throw std::logic_error("cannot delinearize unknown type");

		return nullptr;
	}

	Type* SemaFactory::UPtrToType(const Type* pointee)
	{
		auto it = pointers_.find(pointee);
		if (it != pointers_.end())
		{
			return it->second;
		}
		else
		{
			std::string name(pointee->name());
			if (name.empty())
				name = generateUniqueName("annonptr");

			// oooh ... this is fugly
			auto t = const_cast<Scope*>(pointee->scope())->addType(name+'*', std::make_unique<PointerType>(pointee->scope(), pointee, name));
			pointers_.insert({ pointee, t });
			return t;
		}
	}
	Type* SemaFactory::UArrayOfType(Type* arrayOf, unsigned size)
	{
		auto it = arrays_.find({ arrayOf,size });
		if (it != arrays_.end())
		{
			return it->second;
		}
		else
		{
			//std::string name = arrayOf->name().empty() ? generateUniqueName("annonarray") : arrayOf->name();
			std::string name = generateUniqueName("annonarray");
			// oooh ... this is fugly
			auto t = const_cast<Scope*>(arrayOf->scope())->addType(name+"[]", std::make_unique<ArrayType>(arrayOf->scope(), arrayOf, size, name));
			arrays_.insert({ std::make_pair(arrayOf,size), t });
			return t;
		}
	}
}