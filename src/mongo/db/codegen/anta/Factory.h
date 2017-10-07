#pragma once

#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/Scope.h"
#include "mongo/db/codegen/anta/Expr.h"


#include <memory>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

namespace std
{
	template<>
	struct hash < std::pair<anta::Type*, unsigned> >
	{
		std::size_t operator() (const std::pair<anta::Type*, unsigned>& v) const
		{
			return hash<void*>()(v.first) ^ hash<unsigned>()(v.second);
		}
	};
}
template<class _Ty,
	class... _Types> inline
	boost::intrusive_ptr<_Ty> make_intrusive(_Types&&... _Args)
{	// make an intrusive_ptr
	return (boost::intrusive_ptr<_Ty>(new _Ty(std::forward<_Types>(_Args)...)));
}

namespace anta
{
    // create all semantic objects and manages their lifetime
    struct SemaFactory
    {
		friend class Cloner;
    private:
        unsigned nameCounter_;

		// the root global scope
        std::unique_ptr<Scope> globalScope_;

		// all pointer types
		std::unordered_map<const Type*, Type*> pointers_;

		// all array types
		std::unordered_map<std::pair<Type*,unsigned>, Type*> arrays_;

		// all structs defined
		std::vector<std::pair<Scope*, StructType*>> structs_;

		// all int constants - unified
		std::unordered_map<int, intrusive_ptr<const Expr>> intConstants_;

		// all string constants - unified
		std::unordered_map<std::string, intrusive_ptr<const Expr>> stringConstants_;
		
		// all double constants - unified
		std::unordered_map<double, intrusive_ptr<const Expr>> doubleConstants_;

		// all nullptrs - unified
		std::unordered_map<const Type*, intrusive_ptr<const Expr>> nullptrs_;
		
		// all function pointers - unified
		std::unordered_map<Function*, intrusive_ptr<const Expr>> funptrs_;

		// all functions
        std::vector<Function*> functions_;

		// all placeholder environments
		std::vector<std::unique_ptr<ExprPlaceholders>> placeholders_;

        Scope* createScope(const std::string& name, Scope* parent);

		const anta::StructType* checkStructType(const boost::intrusive_ptr<const anta::Expr> & ptr);
		intrusive_ptr<const Expr> addCastIfNeeded(const intrusive_ptr<const Expr>& expr, const Type* desiredType);
		std::vector<intrusive_ptr<const Expr>>&& addCastIfNeeded(const Function* fn, std::vector<intrusive_ptr<const Expr>>&& args, int offset);

	public:
        SemaFactory();

        Scope* globalScope() const
        {
            return globalScope_.get();
        }

		std::string generateUniqueName(const std::string& name);
		
		Scope* createScope(Scope* parent, const std::string& name = "");

		Variable* createVariable(Scope* scope, const Type* type, std::string name, bool ssa = false);
        Function* createFunction(Scope* scope, const Type* ret, const std::string& name, void* builtin = nullptr);
		Function* createFunction(Scope* scope, const std::string& name, void* builtin = nullptr);
		ExprPlaceholders* createPlaceholders(const std::vector<const anta::Type*>& types); // this should be ref counted otherwise we leak during mfunc inlining

		void destroyFunction(Function* f);

        // short cuts
        // types are unified
		Type* UInt1Type() { return globalScope_->getType("int1"); }
		Type* UIntType() { return globalScope_->getType("int"); }
		Type* UInt8Type() { return globalScope_->getType("int8"); }
		Type* UInt16Type() { return globalScope_->getType("int16"); }
		Type* UInt64Type() { return globalScope_->getType("int64"); }
		Type* UDoubleType() { return globalScope_->getType("double"); }
        Type* UStringType(bool weak=false) { return weak ? globalScope_->getType("weak_string") : globalScope_->getType("string"); }
		Type* UPtrToIntType() { return UPtrToType(UIntType()); }
		Type* UPtrToInt8Type() { return UPtrToType(UInt8Type()); }
		Type* UPtrToType(const Type* pointee);
		Type* UArrayOfType(Type* arrayOf, unsigned size);
		StructType* UStructType(Scope* scope, const std::string& name);

		Type* GlobalType(const std::string& name) { return globalScope_->getType(name); }
		Function* GlobalFunction(const std::string& name) { return globalScope_->getFunction(name); }

		const Type* delinearize(const Type* t);

        Function* printInt() { return globalScope_->getFunction("printInt"); }
		Function* printPointer() { return globalScope_->getFunction("printPointer"); }
		Function* printString() { return globalScope_->getFunction("printString"); }

		// for cloning only
		intrusive_ptr<const Expr> Unary(const Type* t, ExprOp op, const intrusive_ptr<const Expr>& expr);
		intrusive_ptr<const Expr> Binary(const Type* t, ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs);
		
		// constants are unified
		intrusive_ptr<const Expr> UNullPtr(const Type* t);
		intrusive_ptr<const Expr> UFuncPtr(Function* f);
		intrusive_ptr<const Expr> UIntConst(const int i);
		intrusive_ptr<const Expr> UStringConst(const std::string& s);
		intrusive_ptr<const Expr> UDoubleConst(const double d);
		intrusive_ptr<const Expr> CastInteger(const intrusive_ptr<const Expr>& e, const Type* t);

		intrusive_ptr<const Expr> BitCast(const intrusive_ptr<const Expr>& e, const Type* t);
		intrusive_ptr<const Expr> Var(Variable* v);
		intrusive_ptr<const Expr> Cmp(ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs);
		intrusive_ptr<const Expr> Logic(ExprOp op, const intrusive_ptr<const Expr>& expr);
		intrusive_ptr<const Expr> Logic(ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs);
		intrusive_ptr<const Expr> Arith(ExprOp op, const intrusive_ptr<const Expr>& expr);
		intrusive_ptr<const Expr> Arith(ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs);
		intrusive_ptr<const Expr> BitOp(ExprOp op, const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs);
		intrusive_ptr<const Expr> FuncExpr(const Function* fn, std::vector<intrusive_ptr<const Expr>>&& args);
		intrusive_ptr<const Expr> Dereference(const intrusive_ptr<const Expr>& ptr);
		intrusive_ptr<const Expr> AddressOf(const intrusive_ptr<const Expr>& ptr);
		intrusive_ptr<const Expr> StructAccess(const intrusive_ptr<const Expr>& ptr, const std::string& field);
		intrusive_ptr<const Expr> StructAccess(const intrusive_ptr<const Expr>& ptr, int ordinal);
		intrusive_ptr<const Expr> Hole(const ExprPlaceholders* env, unsigned slot);
		intrusive_ptr<const Expr> InlinedFunc(const intrusive_ptr<const Expr>& returnExpr, const intrusive_ptr<Statement>& body);
		
		intrusive_ptr<Statement> Assign(const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs, bool initial = false);
		intrusive_ptr<Statement> For(const intrusive_ptr<Statement>& init, const intrusive_ptr<const Expr>& condition, const intrusive_ptr<Statement>& step, const intrusive_ptr<Statement>& body);
		intrusive_ptr<Statement> If(const intrusive_ptr<const Expr>& cond, const intrusive_ptr<Statement>& then, const intrusive_ptr<Statement>& else_);
		intrusive_ptr<Statement> FuncStmt(const Function* fn, std::vector<intrusive_ptr<const Expr>>&& args);
		intrusive_ptr<Statement> Return(const intrusive_ptr<const Expr>& ret = nullptr);
		intrusive_ptr<Statement> Stmts(std::vector<intrusive_ptr<Statement>>&& stmts);
		intrusive_ptr<Statement> Switch(const intrusive_ptr<const Expr>& val, std::vector<intrusive_ptr<Statement>>&& branches);
		intrusive_ptr<Statement> MFuncStmt(const Function* fn, std::vector<intrusive_ptr<const Expr>>&& args, std::vector<FuncletDef>&& branches);
		intrusive_ptr<Statement> MReturn(FuncletRet&& return_);
		intrusive_ptr<Statement> Break(int level);
		intrusive_ptr<Statement> Continue();
		intrusive_ptr<Statement> Block(const intrusive_ptr<Statement>& body, bool loop);
		intrusive_ptr<Statement> Noop();

		void runOptimizations(CodeGenContext& ctx);
		void generateStructTypes(CodeGenContext& ctx);
		void generatePrototypes(CodeGenContext& ctx);
        void generateFunctions(CodeGenContext& ctx);
		void finalizeCodeGen(CodeGenContext& ctx);
    };
}
