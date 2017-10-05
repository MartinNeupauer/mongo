#pragma once
#include "Sema.h"
#include "ReplaceStmt.h"

#include <unordered_set>
namespace anta
{
	class InlineMFunc : public Replacer
	{
		std::unordered_set<const anta::Function*> functions_;
		Function& callerFn_;

		intrusive_ptr<Statement> doInline(MFuncCallStmt* caller);
	public:
		// fn - a function where inlining will take place
		// function - this function will be inlined in a body of fn if called
		InlineMFunc(SemaFactory& f, Function& fn, const anta::Function* function) : Replacer(f), callerFn_(fn) { functions_.insert(function); }
		InlineMFunc(SemaFactory& f, Function& fn, const std::unordered_set<const anta::Function*>& functions) : Replacer(f), functions_(functions), callerFn_(fn) {}

		void run();

		virtual void visit(MFuncCallStmt* caller) override;
	};
}