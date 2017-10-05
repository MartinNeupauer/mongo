#include "LowerMFunc.h"
#include "Function.h"

#include <vector>
namespace anta
{
	void LowerMFunc::visit(MFuncCallStmt * caller)
	{
		// already replaced
		if (stmtMap_.count(caller)) return;

		// run the base version
		Replacer::visit(caller);

		std::vector<intrusive_ptr<Statement>> result;

		auto& callee = *caller->function();
		const auto& returns = callee.returns();

		if (returns.size() != caller->branches().size())
		{
			throw std::logic_error("caller and callee return branches do not match");
		}

		auto scope = f_.createScope(callerFn_.paramScope());

		// create anonymous structs that will hold results
		std::vector<Type*> retstructs;
		unsigned retslotSize = 0;
		unsigned retslotAlign = 0;

		for (unsigned b = 0; b < returns.size(); ++b)
		{
			if (returns[b].returnTypes_.size())
			{
				auto retstruct = f_.UStructType(scope, "");
				retstruct->addFields(returns[b].returnTypes_);
				retslotSize = std::max(retslotSize, retstruct->allocSize());
				retslotAlign = std::max(retslotAlign, retstruct->alignSize());

				retstructs.push_back(retstruct);
			}
			else
			{
				retstructs.push_back(nullptr);
			}
		}
		// union of all structs
		// optimize for all empty structures and use the alignment!
		auto retslot = f_.createVariable(scope, f_.UArrayOfType(f_.UInt8Type(), retslotSize), "retslot");

		// dispatch tag
		auto tag = f_.createVariable(scope, f_.UIntType(), "tag", true);

		std::vector<intrusive_ptr<const Expr>> arguments({ f_.BitCast(f_.AddressOf(f_.Var(retslot)), f_.UPtrToInt8Type()) });
		for (const auto& e : caller->arguments())
			arguments.push_back(e);

		// the real call
		auto call = f_.Assign
		(

			f_.Var(tag),
			f_.FuncExpr(caller->function(), std::move(arguments)),
			true
		);
		result.push_back(call);

		std::vector<intrusive_ptr<Statement>> branches;
		for (unsigned b = 0; b < returns.size(); ++b)
		{
			std::vector<intrusive_ptr<Statement>> branchStmts;
			auto& branch = caller->branches()[b];

			// assign return values
			for (unsigned i = 0; i < branch.returnExprs_.size(); ++i)
			{
				branchStmts.push_back(f_.Assign
				(
					branch.returnExprs_[i],
					f_.StructAccess(f_.BitCast(f_.AddressOf(f_.Var(retslot)), f_.UPtrToType(retstructs[b])), i),
					true)
				);
			}

			// append the rest of the original statements
			branchStmts.push_back(branch.stmt_);

			// and add it to the vector
			branches.push_back(f_.Stmts(std::move(branchStmts)));
		}
		auto dispatch = f_.Switch(f_.Var(tag), std::move(branches));
		result.push_back(dispatch);

		stmtMap_.insert({ caller, f_.Stmts(std::move(result)) });
	}
	void LowerMFunc::visit(MReturnStmt * stmt)
	{
		// already replaced
		if (stmtMap_.count(stmt)) return;

		// run the base version
		Replacer::visit(stmt);

		std::vector<intrusive_ptr<Statement>> result;

		// find the right return branch
		auto& returns = callerFn_.returns();
		auto& ret = stmt->returnTarget();

		unsigned b;
		for (b = 0; b < returns.size(); ++b)
			if (ret.label_ == returns[b].label_)
				break;

		if (b == returns.size())
		{
			throw std::logic_error("return branch not found");
		}

		auto scope = f_.createScope(callerFn_.paramScope());

		// create anonymous struct
		StructType* retstruct = nullptr;
		if (returns[b].returnTypes_.size())
		{
			retstruct = f_.UStructType(scope, "");
			retstruct->addFields(returns[b].returnTypes_);
		}

		// assign return values
		for (unsigned i = 0; i < ret.returnExprs_.size(); ++i)
		{
			result.push_back(
				f_.Assign(

					f_.StructAccess(f_.BitCast(f_.Var(callerFn_.parameters()[0]), f_.UPtrToType(retstruct)), i),
					ret.returnExprs_[i],
					true)
			);
		}

		// return tag
		auto retStmt = f_.Return(f_.UIntConst(b));

		result.push_back(retStmt);

		stmtMap_.insert({ stmt, f_.Stmts(std::move(result)) });
	}
}