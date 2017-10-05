#pragma once
#include "Sema.h"
#include "ReplaceStmt.h"

#include <unordered_set>
namespace anta
{
	class InlineFunction : public Replacer
	{
		Function& callerFn_;
		std::unordered_set<const anta::Function*> functions_;

		intrusive_ptr<Statement> doInline(const Function* fn, const std::vector<intrusive_ptr<const Expr>>& arguments, const intrusive_ptr<const Expr>& ret);
	public:
		// fn - a function where inlining will take place
		// function - this function will be inlined in a body of fn if called
		InlineFunction(SemaFactory& f, Function& fn, const anta::Function* function) : Replacer(f), callerFn_(fn) { functions_.insert(function); }
		InlineFunction(SemaFactory& f, Function& fn, const std::unordered_set<const anta::Function*>& functions) : Replacer(f), callerFn_(fn), functions_(functions) {}

		void run();

		virtual void visit(FuncCallStmt* caller) override;
		virtual void visit(const FuncCallExpr* caller) override;
	};
}