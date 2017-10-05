#include "mongo/db/codegen/anta/InlineMFunc.h"

#include "mongo/db/codegen/anta/Analysis.h"
#include "mongo/db/codegen/anta/Cloner.h"
#include "mongo/db/codegen/anta/Factory.h"

#include <vector>
#include <unordered_map>

namespace anta
{
	class MReturnCloner : public Cloner
	{
		const std::unordered_map<std::string, unsigned>& labels_;
		const std::unordered_map<std::string, std::vector<Variable*>>& retvars_;

		std::unordered_set<unsigned> referencedReturns_;
	public:
		MReturnCloner(
			SemaFactory& f,
			Function& fn,
			const std::unordered_map<std::string, unsigned>& labels,
			const std::unordered_map<std::string, std::vector<Variable*>>& retvars,
			const std::unordered_map<const Scope*, Scope*>& scopeMap,
			const std::unordered_map<const Variable*, Variable*>& variableMap
		)
			: Cloner(f, fn, scopeMap, variableMap), labels_(labels), retvars_(retvars) {}

		auto& referencedReturns() const { return referencedReturns_; }

		using Cloner::visit;
		
		virtual void visit(MReturnStmt *original) override
		{
			if (stmtMap_.count(original)) return;

			std::vector<intrusive_ptr<Statement>> cloned;
			auto& target = original->returnTarget();
			for (unsigned i = 0; i<target.returnExprs_.size(); ++i)
				cloned.push_back(
					f_.Assign
					(

						f_.Var(retvars_.at(target.label_)[i]),
						exprMap_.at(target.returnExprs_[i].get()),
						true
					)
				);

			unsigned nestLevel = 0;
			for (auto s : stmtStack_)
			{
				if (s->is_a<BlockStmt>())
				{
					++nestLevel;
				}
			}

			referencedReturns_.insert(labels_.find(target.label_)->second);
			auto breakStmt = f_.Break(labels_.find(target.label_)->second + nestLevel);

			cloned.push_back(breakStmt);

			stmtMap_.insert({ original, f_.Stmts(std::move(cloned)) });
		}
	};

	intrusive_ptr<Statement> InlineMFunc::doInline(MFuncCallStmt * caller)
	{
		std::unordered_map<const Scope*, Scope*> scopeMap;

		auto paramScope = f_.createScope(callerFn_.paramScope(), callerFn_.name());
		scopeMap.insert({ caller->function()->paramScope(), paramScope });
		std::vector<intrusive_ptr<Statement>> result;

		// create assignemt from arguments to parameters
		auto& parameters = caller->function()->parameters();
		auto& arguments = caller->arguments();
		std::unordered_map<const Variable*, Variable*> variableMap;

		// skip 0th "out" parameter
		for (unsigned i = 1; i < parameters.size(); ++i)
		{
			auto original = parameters[i];
			auto clone = f_.createVariable(paramScope, original->type(), original->name(), original->isssa());
			variableMap.insert({ original, clone });

			result.push_back
			(
				f_.Assign(f_.Var(clone), arguments[i - 1], true)
			);
		}

		// create return variables (fake structs) for all branches
		auto& returns = caller->function()->returns();
		auto returnScope = f_.createScope(callerFn_.paramScope());

		std::unordered_map<std::string, std::vector<Variable*>> retvars;
		for (unsigned b = 0; b < returns.size(); ++b)
		{
			std::vector<Variable*> vars;
			std::string name = "r_";
			name += std::to_string(b); name += "_";
			for (unsigned i = 0; i < returns[b].returnTypes_.size(); ++i)
			{
				auto v = f_.createVariable(returnScope, returns[b].returnTypes_[i], name + std::to_string(i));
				vars.push_back(v);
			}
			retvars.insert({ returns[b].label_, std::move(vars) });
		}

		// create branches for return funclets and read from return variables
		std::vector<intrusive_ptr<Statement>> branches;
		std::unordered_map<std::string, unsigned> labels;
		for (unsigned b = 0; b < returns.size(); ++b)
		{
			std::vector<intrusive_ptr<Statement>> branchStmts;
			auto& branch = caller->branches()[b];

			labels.insert({ branch.label_, returns.size()-1 - b });

			// assign return values
			for (unsigned i = 0; i < branch.returnExprs_.size(); ++i)
			{
				branchStmts.push_back(f_.Assign
				(

					branch.returnExprs_[i],
					f_.Var(retvars[branch.label_][i]),
					true)
				);
			}

			// append the rest of the original statements
			branchStmts.push_back(branch.stmt_);

			FallthruAnalysis fa;
			if (!fa.exitEarly(branchStmts))
			{
				// final break(x) statement
				branchStmts.push_back(f_.Break(b));
			}

			// and add it to the vector
			branches.push_back(f_.Stmts(std::move(branchStmts)));
		}

		// clone the function body and replace 
		// 1. all paramener variables with new local variables
		// 2. all mreturn statements with assignemt to return variables and jump to a label

		MReturnCloner cloner(f_, callerFn_, labels, retvars, scopeMap, variableMap);

		auto body = f_.Block(cloner.clone(caller->function()->body_.get()), false);

		// wrap everything in Blocks backward
		for (unsigned b = branches.size(); b-- > 0;)
		{
			if (cloner.referencedReturns().count(returns.size() - 1 - b))
				body = f_.Block(f_.Stmts({body, branches[b]}), false);
			else
				body = f_.Block(body, false);
		}

		result.push_back(body);

		return f_.Stmts(std::move(result));
	}

	void InlineMFunc::run()
	{
		callerFn_.body_->postorder(*this);
		auto it = stmtMap_.find(callerFn_.body_.get());
		if (it != stmtMap_.end())
			callerFn_.body_ = it->second;
	}

	void InlineMFunc::visit(MFuncCallStmt * caller)
	{
		// inline if callee is in a set of functions to inline
		if (functions_.count(caller->function()))
		{
			stmtMap_.insert({ caller, doInline(caller) });
		}
	}
}