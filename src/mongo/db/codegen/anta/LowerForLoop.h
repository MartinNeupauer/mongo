#pragma once
#include "mongo/db/codegen/anta/Factory.h"
#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/ReplaceStmt.h"

#include <unordered_map>
#include <unordered_set>

namespace anta
{
	class LowerForLoop : public Replacer
	{
	public:
		LowerForLoop(SemaFactory& f) : Replacer(f) {}

		intrusive_ptr<Statement> run(
			const intrusive_ptr<Statement>& init,
			const intrusive_ptr<const Expr>& condition,
			const intrusive_ptr<Statement>& step,
			intrusive_ptr<Statement> body);

		using Replacer::visit;
		virtual void visit(ContinueStmt* stmt) override;
		virtual void visit(BreakStmt* stmt) override;
	};
}