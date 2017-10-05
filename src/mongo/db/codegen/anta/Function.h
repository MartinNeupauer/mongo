#pragma once
#include "mongo/db/codegen/anta/CodeGenCtx.h"
#include "mongo/db/codegen/anta/Type.h"
#include "mongo/db/codegen/anta/Statement.h"
#include "mongo/db/codegen/anta/Variable.h"

#include <string>
#include <vector>

namespace llvm
{
    class Function;
	class GlobalVariable;
}

namespace anta
{
	struct FuncletDecl
	{
		std::string	             label_;
		std::vector<const Type*> returnTypes_;
	};

    class Function
    {
		Scope*                  scope_;      //scope where the function is declared
		const Type*				functionType_;
		const Type*             returnType_;     //for single return value functions, I have to revisit it
        const std::string       name_;
        Scope*                  paramScope_; //parameters
		std::vector<Variable*>  parameters_;
		std::vector<FuncletDecl>
			                    returns_;
        void*                   builtin_;
		llvm::Function*			llvmfn_;
		llvm::GlobalVariable*   globalVar_; // non-null if this function is coming from an external code
		unsigned				intrinsicId_;
		bool					const_;
		bool					inline_;

		void generateVariables(const Scope* scope, EnvCodeGenCtx& ectx) const;
	public:
		intrusive_ptr<Statement>              body_;
		Function(Scope* scope, const Type* type, Scope* paramScope, const std::string& name, void* builtin = nullptr);

        void setParameters(const std::vector<Variable*>& params) { parameters_ = params; }
		void addParameter(Variable* param) { parameters_.push_back(param); }
		void setReturns(const std::vector<FuncletDecl>& returns) { returns_ = returns; }
		void addReturn(FuncletDecl ret) { returns_.push_back(std::move(ret)); }

		void setAndCheck(intrusive_ptr<Statement> body);

		const auto& returns() const { return returns_; }
		const auto& parameters() const { return parameters_; }

        Scope* paramScope() const { return paramScope_; }

		Scope* scope() const { return scope_; }
		const Type* retType() const { return returnType_; }
		const Type* type() const { return functionType_; }

		bool isIntrinsic() const { return intrinsicId_ != 0; }
		bool isBuiltin() const { return builtin_ != nullptr; }

		void setConst() { const_ = true; }

		void setInline() { inline_ = true; }
		bool isInline() const { return inline_; }

		const std::string& name() const { return name_; }
        std::string fullName() const;

		llvm::Function* getllvmfn() const { return llvmfn_; }
		llvm::GlobalVariable* getGlobalVar() const { return globalVar_; }

        llvm::Function* generateProto(CodeGenContext& ctx);
        void generate(CodeGenContext& ctx) const;
    };
}
