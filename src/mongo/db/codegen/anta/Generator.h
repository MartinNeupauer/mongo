#pragma once
#include "mongo/db/codegen/anta/Expr.h"
#include "mongo/db/codegen/anta/Factory.h"
#include "mongo/db/codegen/anta/Function.h"
#include "mongo/db/codegen/anta/Statement.h"
#include "mongo/db/codegen/anta/LowerMFunc.h"
#include "mongo/db/codegen/anta/InlineMFunc.h"

#ifndef BOOST_NO_IOSTREAM
#define BOOST_NO_IOSTREAM
#endif
#include <boost/variant.hpp>
#include <vector>

namespace anta
{
	using namespace boost;

	class Generator;

	class Wrapper
	{
	public:
		Generator& g_;
		intrusive_ptr<const anta::Expr> expr_;
		intrusive_ptr<anta::Statement> stmt_;

		Wrapper() = delete;
		Wrapper(const Wrapper&) = default;
		Wrapper(Wrapper&&) = default;

		Wrapper(Generator& g) : g_(g), expr_(nullptr), stmt_(nullptr) {}
		Wrapper(Generator& g, intrusive_ptr<const anta::Expr>&& expr) : g_(g), expr_(std::move(expr)), stmt_(nullptr) {}
		Wrapper(Generator& g, intrusive_ptr<anta::Statement>&& stmt) : g_(g), expr_(nullptr), stmt_(std::move(stmt)) {}

		~Wrapper();
		Wrapper operator= (const Wrapper& rhs) const;
		Wrapper operator* () const;
		Wrapper operator& () const;
		Wrapper operator[] (const char* field) const;
		Wrapper operator[] (int field) const;
	};

	enum class FrameType
	{
		normalFrame,
		forFrame,
		ifFrame,
		functionFrame,
		mcallFrame,
		mcallbranchFrame,
	};

	struct ForParameters
	{
		static const FrameType ftype_ = FrameType::forFrame;

		intrusive_ptr<anta::Statement> init_;
		intrusive_ptr<const anta::Expr> condition_;
		intrusive_ptr<anta::Statement> step_;
	};

	struct IfParameters
	{
		static const FrameType ftype_ = FrameType::ifFrame;

		intrusive_ptr<const anta::Expr> condition_;
		intrusive_ptr<anta::Statement> then_; // optional if we don't have an 'else' branch
	};

	struct FunctionParameters
	{
		static const FrameType ftype_ = FrameType::functionFrame;

		anta::Function* function_;
	};

	struct MCallParameters
	{
		static const FrameType ftype_ = FrameType::mcallFrame;

		const anta::Function* function_;
		std::vector<intrusive_ptr<const anta::Expr>> args_;
		std::vector<anta::FuncletDef> branches_;
	};

	struct MCallBranchParameters
	{
		static const FrameType ftype_ = FrameType::mcallbranchFrame;

		anta::FuncletDef branch_;
	};

	class Generator
	{
		friend class Wrapper;

		struct Frame
		{
			FrameType type_;

			anta::Scope* scope_;
			std::vector<intrusive_ptr<anta::Statement>> stmts_;

			boost::variant
			<
				ForParameters,
				IfParameters,
				FunctionParameters,
				MCallParameters,
				MCallBranchParameters
			> p_;

			Frame(anta::Scope* scope) : type_(FrameType::normalFrame), scope_(scope) {}

			template <typename T>
			void set(const T& p)
			{
				if (type_ != FrameType::normalFrame)
					throw std::logic_error("frame already set");

				type_ = T::ftype_;
				p_ = p;
			}
		};

		// stack of statement currently being built frames
		std::vector<Frame> frames_;

		anta::Scope* currentScope() const { return frames_.back().scope_; }
		Frame& currentFrame() { return frames_.back(); }

		void pushFrame(anta::Scope* scope = nullptr)
		{
			if (scope && scope->parent() != currentScope())
				throw std::logic_error("broken scope link");

			frames_.emplace_back(Frame(scope ? scope : factory_.createScope(currentScope())));
		}

		auto popFrame()
		{
			auto ret = factory_.Stmts(std::move(frames_.back().stmts_));
			frames_.pop_back();

			return ret;
		}

		void addToFrame(intrusive_ptr<anta::Statement> s)
		{
			if (!s)
				return;

			frames_.back().stmts_.push_back(std::move(s));
		}

		auto varInternal_(anta::Variable* var)
		{
			return Wrapper(*this, factory_.Var(var));
		}

	protected:
		auto nothing_()
		{
			return Wrapper(*this);
		}

		auto var_(const anta::Type* type, const char* name)
		{
			bool ssa = name[0] == '%';

			auto var = factory_.createVariable(currentScope(), type, name, ssa);
			return Wrapper(*this, factory_.Var(var));
		}

		auto var_(const char* name, Wrapper init)
		{
			auto initExpr = std::move(init.expr_);

			auto var = var_(initExpr->type(), name);
			addToFrame(factory_.Assign(var.expr_, initExpr, true));
			return var;
		}

		auto var_(const char* name)
		{
			return varInternal_(currentScope()->getVariable(name));
		}

		auto param_(const anta::Type* type, const char* name)
		{
			if (currentFrame().type_ != FrameType::functionFrame)
				throw std::logic_error("function frame expected");

			FunctionParameters& p = boost::get<FunctionParameters>(currentFrame().p_);

			bool ssa = name[0] == '%';

			auto var = factory_.createVariable(currentScope(), type, name, ssa);
			p.function_->addParameter(var);

			return varInternal_(var);
		}

		void return_(const char* label, std::vector<const anta::Type*> types)
		{
			if (currentFrame().type_ != FrameType::functionFrame)
				throw std::logic_error("function frame expected");

			FunctionParameters& p = boost::get<FunctionParameters>(currentFrame().p_);

			p.function_->addReturn({label, std::move(types)});
		}

		auto nullptr_(const anta::Type* t)
		{
			return Wrapper(*this, factory_.UNullPtr(t));
		}

		auto funcptr_(anta::Function* f)
		{
			return Wrapper(*this, factory_.UFuncPtr(f));
		}

		auto const_(int c)
		{
			return Wrapper(*this, factory_.UIntConst(c));
		}

		auto const1_(bool c)
		{
			return Wrapper(*this, factory_.CastInteger(factory_.UIntConst(c), int1_));
		}

		auto const8_(int c)
		{
			return Wrapper(*this, factory_.CastInteger(factory_.UIntConst(c), int8_));
		}

		auto const64_(int c)
		{
			return Wrapper(*this, factory_.CastInteger(factory_.UIntConst(c), int64_));
		}

		auto constd_(double c)
		{
			return Wrapper(*this, factory_.UDoubleConst(c));
		}
		
		auto const_(const std::string& s)
		{
			return Wrapper(*this, factory_.UStringConst(s));
		}

		auto cast_(const anta::Type* type, Wrapper expr)
		{
			return Wrapper(*this, factory_.BitCast(expr.expr_, type));
		}

		auto eval_(const anta::Function* fn, std::vector<Wrapper> args)
		{
			std::vector<boost::intrusive_ptr<const anta::Expr>> argsExpr;
			for (auto& w : args)
				argsExpr.push_back(std::move(w.expr_));

			return Wrapper(*this, factory_.FuncExpr(fn, std::move(argsExpr)));
		}

		auto eval_(const char* name, std::vector<Wrapper> args)
		{
			return eval_(currentScope()->getFunction(name), std::move(args));
		}

		auto mfunction_(const std::string& name)
		{
			// always return int
			auto fn = factory_.createFunction(currentScope(), int_, name);

			pushFrame(fn->paramScope());
			currentFrame().set(FunctionParameters{ fn });

			// 0th paramater is always int8*
			param_(factory_.UPtrToType(int8_), "%out");

			return fn;
		}

		auto function_(const anta::Type* retType, const std::string& name, void* builtin = nullptr)
		{
			auto fn = factory_.createFunction(currentScope(), retType, name, builtin);

			pushFrame(fn->paramScope());
			currentFrame().set(FunctionParameters{ fn });

			return fn;
		}

		auto function_(const std::string& name, void* builtin = nullptr)
		{
			return function_(nullptr, name, builtin);
		}

		void body_()
		{
			if (currentFrame().type_ != FrameType::functionFrame)
				throw std::logic_error("function frame expected");

			currentFrame().scope_ = factory_.createScope(currentFrame().scope_);
		}

		void endfunction_()
		{
			if (currentFrame().type_ != FrameType::functionFrame)
				throw std::logic_error("function frame expected");

			FunctionParameters p = boost::get<FunctionParameters>(currentFrame().p_);
			if (p.function_->isIntrinsic() || p.function_->isBuiltin())
			{
				popFrame();
			}
			else
			{
				p.function_->setAndCheck(popFrame());
			}
		}

		void loop_()
		{
			for_(Wrapper(*this, (anta::Statement*)nullptr), Wrapper(*this, (anta::Expr*)nullptr), Wrapper(*this, (anta::Statement*)nullptr));
		}

		void for_(Wrapper init, Wrapper cond, Wrapper step)
		{
			pushFrame();
			currentFrame().set(ForParameters{ std::move(init.stmt_), std::move(cond.expr_), std::move(step.stmt_) });
		}

		void endfor_()
		{
			if (currentFrame().type_ != FrameType::forFrame)
				throw std::logic_error("for frame expected");

			ForParameters p{ std::move(boost::get<ForParameters>(currentFrame().p_)) };
			addToFrame(factory_.For(p.init_, p.condition_, p.step_, popFrame()));
		}

		void if_(Wrapper cond)
		{
			pushFrame();
			currentFrame().set(IfParameters{std::move(cond.expr_), nullptr});
		}

		void endif_()
		{
			if (currentFrame().type_ != FrameType::ifFrame)
				throw std::logic_error("if frame expected");

			IfParameters p{ std::move(boost::get<IfParameters>(currentFrame().p_)) };
			if (!p.then_)
				addToFrame(factory_.If(p.condition_, popFrame(), factory_.Noop()));
			else
				addToFrame(factory_.If(p.condition_, p.then_, popFrame()));
		}
		void else_()
		{
			if (currentFrame().type_ != FrameType::ifFrame)
				throw std::logic_error("if frame expected");

			IfParameters& p = boost::get<IfParameters>(currentFrame().p_);

			if (p.then_)
				throw std::logic_error("multiple else statements");

			p.then_ = factory_.Stmts(std::move(currentFrame().stmts_));

			currentFrame().stmts_.clear();
		}

		void return_(Wrapper ret)
		{
			addToFrame(factory_.Return(ret.expr_));
		}

		void return_()
		{
			addToFrame(factory_.Return());
		}

		void mreturn_(const char* label, std::vector<Wrapper> args = {})
		{
			std::vector<intrusive_ptr<const anta::Expr>> argsExpr;
			for (auto& w : args)
				argsExpr.push_back(std::move(w.expr_));

			addToFrame(factory_.MReturn({ label, std::move(argsExpr) }));
		}

		void call_(const anta::Function* fn, std::vector<Wrapper> args)
		{
			std::vector<intrusive_ptr<const anta::Expr>> argsExpr;
			for (auto& w : args)
				argsExpr.push_back(std::move(w.expr_));

			addToFrame(factory_.FuncStmt(fn, std::move(argsExpr)));
		}

		void call_(const char* name, std::vector<Wrapper> args)
		{
			call_(currentScope()->getFunction(name), std::move(args));
		}

		void mcall_(const anta::Function* fn, std::vector<Wrapper> args)
		{
			std::vector<intrusive_ptr<const anta::Expr>> argsExpr;
			for (auto& w : args)
				argsExpr.push_back(std::move(w.expr_));

			pushFrame();
			currentFrame().set(MCallParameters{ fn, std::move(argsExpr), {} });
		}

		void break_()
		{
			addToFrame(factory_.Break(-1));
		}

		void continue_()
		{
			addToFrame(factory_.Continue());
		}

		void paste_(intrusive_ptr<anta::Statement> stmt)
		{
			addToFrame(stmt);
		}

		void endmcall_()
		{
			if (currentFrame().type_ != FrameType::mcallFrame)
				throw std::logic_error("mcall frame expected");

			MCallParameters p{ std::move(boost::get<MCallParameters>(currentFrame().p_)) };

			auto checkStmt = popFrame();
			if (checkStmt && !checkStmt->is_a<anta::NoopStmt>())
				throw std::logic_error("unexpected statement");

			addToFrame(factory_.MFuncStmt(p.function_, std::move(p.args_), std::move(p.branches_)));
		}

		void branch_(const char* label, std::vector<Wrapper> args = {})
		{
			if (currentFrame().type_ != FrameType::mcallFrame)
				throw std::logic_error("mcall frame expected");

			std::vector<intrusive_ptr<const anta::Expr>> argsExpr;
			for (auto& w : args)
				argsExpr.push_back(std::move(w.expr_));

			pushFrame();
			currentFrame().set(MCallBranchParameters{ {label, std::move(argsExpr), nullptr} });
		}

		void endbranch_()
		{
			if (currentFrame().type_ != FrameType::mcallbranchFrame)
				throw std::logic_error("mcallbranch frame expected");

			MCallBranchParameters p{ std::move(boost::get<MCallBranchParameters>(currentFrame().p_)) };
			p.branch_.stmt_ = popFrame();
			
			if (currentFrame().type_ != FrameType::mcallFrame)
				throw std::logic_error("mcall frame expected");

			MCallParameters& c = boost::get<MCallParameters>(currentFrame().p_);
			c.branches_.push_back(p.branch_);
		}

		void end_()
		{
			switch (currentFrame().type_)
			{
			case FrameType::forFrame: endfor_(); break;
			case FrameType::ifFrame: endif_(); break;
			case FrameType::functionFrame: endfunction_(); break;
			case FrameType::mcallFrame: endmcall_(); break;
			case FrameType::mcallbranchFrame: endbranch_(); break;
			default:
				throw std::logic_error("unknown frame");
			}
		}

		auto ptr_(const anta::Type* pointee)
		{
			return factory_.UPtrToType(pointee);
		}

		// shortcuts
		const anta::Type* int1_;
		const anta::Type* int8_;
		const anta::Type* int16_;
		const anta::Type* int_;
		const anta::Type* int64_;
		const anta::Type* string_;
		const anta::Type* weak_string_;
		const anta::Type* double_;

		const anta::Type* pint8_;
		const anta::Type* pint16_;
		const anta::Type* pint_;
		const anta::Type* pint64_;
		const anta::Type* pstring_;
		const anta::Type* pweak_string_;
		const anta::Type* pdouble_;

		const anta::Type* variant_;
	public:
		anta::SemaFactory& factory_;

		Generator(anta::SemaFactory& f) : factory_(f)
		{
			frames_.emplace_back(Frame(factory_.globalScope()));
			int1_ = factory_.UInt1Type();
			int8_ = factory_.UInt8Type();
			int16_ = factory_.UInt16Type();
			int_ = factory_.UIntType();
			int64_ = factory_.UInt64Type();
			string_ = factory_.UStringType();
			weak_string_ = factory_.UStringType(true);
			double_ = factory_.UDoubleType();

			pint8_ = ptr_(int8_);
			pint16_ = ptr_(int16_);
			pint_ = ptr_(int_);
			pint64_ = ptr_(int64_);
			pstring_ = ptr_(string_);
			pweak_string_ = ptr_(weak_string_);
			pdouble_ = ptr_(double_);

			variant_ = currentScope()->getType("Variant");
		}
	};

	inline Wrapper::~Wrapper()
	{
		if (stmt_)
			g_.addToFrame(stmt_);
	}

	inline Wrapper Wrapper::operator= (const Wrapper& rhs) const
	{
		return Wrapper(g_, g_.factory_.Assign(expr_, rhs.expr_));
	}

	inline Wrapper Wrapper::operator* () const
	{
		return Wrapper(g_, g_.factory_.Dereference(expr_));
	}

	inline Wrapper Wrapper::operator& () const
	{
		return Wrapper(g_, g_.factory_.AddressOf(expr_));
	}

	inline Wrapper Wrapper::operator[] (const char* field) const
	{
		return Wrapper(g_, g_.factory_.StructAccess(expr_, field));
	}

	inline Wrapper Wrapper::operator[] (int field) const
	{
		return Wrapper(g_, g_.factory_.StructAccess(expr_, field));
	}

	inline Wrapper operator- (const Wrapper& expr) { return Wrapper(expr.g_, expr.g_.factory_.Arith(anta::op_unary_minus, expr.expr_)); }
	inline Wrapper operator! (const Wrapper& expr) { return Wrapper(expr.g_, expr.g_.factory_.Logic(anta::op_logical_not, expr.expr_)); }

	inline Wrapper operator+ (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Arith(anta::op_arit_plus, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator- (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Arith(anta::op_arit_minus, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator* (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Arith(anta::op_arit_mult, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator/ (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Arith(anta::op_arit_div, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator% (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Arith(anta::op_arit_rem, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator<< (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.BitOp(anta::op_bit_leftshift, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator>> (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.BitOp(anta::op_bit_rightshift, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator& (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.BitOp(anta::op_bit_and, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator| (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.BitOp(anta::op_bit_or, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator^ (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.BitOp(anta::op_bit_xor, lhs.expr_, rhs.expr_)); }

	inline Wrapper operator< (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Cmp(anta::op_comp_less, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator> (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Cmp(anta::op_comp_greater, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator== (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Cmp(anta::op_comp_eq, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator!= (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Cmp(anta::op_comp_neq, lhs.expr_, rhs.expr_)); }

	inline Wrapper operator&& (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Logic(anta::op_logical_and, lhs.expr_, rhs.expr_)); }
	inline Wrapper operator|| (const Wrapper& lhs, const Wrapper& rhs) { return Wrapper(lhs.g_, lhs.g_.factory_.Logic(anta::op_logical_or, lhs.expr_, rhs.expr_)); }
}