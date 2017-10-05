#include "mongo/db/codegen/anta/InlineFunction.h"

#include "mongo/db/codegen/anta/Cloner.h"
#include "mongo/db/codegen/anta/Factory.h"

#include <vector>
#include <unordered_map>

namespace anta
{
	class ReturnCloner : public Cloner
	{
		const intrusive_ptr<const Expr>& ret_;

	public:
		ReturnCloner(
			SemaFactory& f,
			Function& fn,
			const intrusive_ptr<const Expr>& ret,
			const std::unordered_map<const Scope*, Scope*>& scopeMap,
			const std::unordered_map<const Variable*, Variable*>& variableMap
		)
			: Cloner(f, fn, scopeMap, variableMap), ret_(ret) {}

		using Cloner::visit;
		virtual void visit(ReturnStmt *original) override
		{
			if (stmtMap_.count(original)) return;

			std::vector<intrusive_ptr<Statement>> cloned;
			if (original->_return())
			{
				cloned.push_back(
					f_.Assign(
						ret_,
						exprMap_.at(original->_return().get()),
						true
					)
				);
			}

			unsigned nestLevel = 0;
			for (auto s : stmtStack_)
			{
				if (s->is_a<BlockStmt>())
				{
					++nestLevel;
				}
			}

			auto breakStmt = f_.Break(nestLevel);

			cloned.push_back(breakStmt);

			stmtMap_.insert({ original, f_.Stmts(std::move(cloned)) });
		}
	};

	intrusive_ptr<Statement> InlineFunction::doInline(const Function* calleeFn, const std::vector<intrusive_ptr<const Expr>>& arguments, const intrusive_ptr<const Expr>& ret)
	{
		std::unordered_map<const Scope*, Scope*> scopeMap;

		auto paramScope = f_.createScope(callerFn_.paramScope(), callerFn_.name());
		scopeMap.insert({ calleeFn->paramScope(), paramScope });
		std::vector<intrusive_ptr<Statement>> result;

		// create assignemt from arguments to parameters
		auto& parameters = calleeFn->parameters();

		std::unordered_map<const Variable*, Variable*> variableMap;
		for (unsigned i = 0; i < parameters.size(); ++i)
		{
			auto original = parameters[i];
			auto clone = f_.createVariable(paramScope, original->type(), original->name(), original->isssa());
			variableMap.insert({ original, clone });

			result.push_back
			(
				f_.Assign(f_.Var(clone), arguments[i], true)
			);
		}

		// clone the function body and replace 
		// 1. all paramener variables with new local variables
		// 2. all return statements with assignemt to return variable and break
		// 3. wrap it in a block
		ReturnCloner cloner(f_, callerFn_, ret, scopeMap, variableMap);
		auto clonedbody = cloner.clone(calleeFn->body_.get());

		result.push_back(f_.Block(clonedbody, false));

		return f_.Stmts(std::move(result));
	}

	void InlineFunction::run()
	{
		callerFn_.body_->postorder(*this);
		auto it = stmtMap_.find(callerFn_.body_.get());
		if (it != stmtMap_.end())
			callerFn_.body_ = it->second;
	}

	void InlineFunction::visit(FuncCallStmt * caller)
	{
		// inline if callee is in a set of functions to inline
		if (functions_.count(caller->function()))
		{
			auto cloned = doInline(caller->function(), caller->arguments(), nullptr);

			stmtMap_.insert({ caller, cloned });
		}
	}
	void InlineFunction::visit(const FuncCallExpr* caller)
	{
		// inline if callee is in a set of functions to inline
		if (functions_.count(caller->function()))
		{
			auto returnScope = f_.createScope(callerFn_.paramScope(), callerFn_.name());

			auto var = f_.createVariable(returnScope, caller->type(), f_.generateUniqueName("retvar"));
			auto ret = f_.Var(var);
			auto cloned = doInline(caller->function(), caller->arguments(), ret);

			auto expr = f_.InlinedFunc(ret, cloned);

			exprMap_.insert({ caller, expr });
		}
		
	}
	
}