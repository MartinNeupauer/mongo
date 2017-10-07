#include "mongo/db/codegen/anta/LowerForLoop.h"
#include "mongo/db/codegen/anta/Function.h"

#include <vector>
namespace anta
{
	intrusive_ptr<Statement> LowerForLoop::run(
		const intrusive_ptr<Statement>& init,
		const intrusive_ptr<const Expr>& condition,
		const intrusive_ptr<Statement>& step,
		intrusive_ptr<Statement> body)
	{
		// replaced for loop
		std::vector<intrusive_ptr<Statement>> result;

		// first run the init if present
		if (init)
			result.push_back(init);

		// the main loop block
		std::vector<intrusive_ptr<Statement>> blockBody;

		// check the loop terminating condition
		if (condition)
		{
			blockBody.push_back(
				f_.If(condition, f_.Noop(), f_.Break(0))
			);
		}

		// the original body nested in another block
		if (body)
		{
			// replace break and continue in the body
			body->postorder(*this);
			auto it = stmtMap_.find(body.get());
			if (it != stmtMap_.end())
				body = it->second;

			blockBody.push_back(
				f_.Block(body, false)
			);
		}

		// run the step if present
		if (step)
		{
			blockBody.push_back(
				step
			);
		}

		result.push_back(f_.Block(f_.Stmts(std::move(blockBody)), true));

		return f_.Stmts(std::move(result));
	}

	void LowerForLoop::visit(ContinueStmt* stmt)
	{
		// already replaced
		if (stmtMap_.count(stmt)) return;

		// run the base version
		Replacer::visit(stmt);

		// replace 'continue' with 'break(0)'
		auto result = f_.Break(0);

		stmtMap_.insert({ stmt, result });
	}

	void LowerForLoop::visit(BreakStmt* stmt)
	{
		// already replaced
		if (stmtMap_.count(stmt)) return;

		if (stmt->valid())
			return;
		
		// run the base version
		Replacer::visit(stmt);

		// replace 'break' with 'break(1)'
		auto result = f_.Break(1);

		stmtMap_.insert({ stmt, result });
	}
}