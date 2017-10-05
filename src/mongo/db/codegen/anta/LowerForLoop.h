#pragma once
#include "Factory.h"
#include "Sema.h"
#include "ReplaceStmt.h"

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

		virtual void visit(ContinueStmt* stmt) override;
		virtual void visit(BreakStmt* stmt) override;
	};
}