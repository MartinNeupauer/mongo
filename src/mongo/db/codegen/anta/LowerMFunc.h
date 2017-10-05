#pragma once
#include "mongo/db/codegen/anta/Factory.h"
#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/ReplaceStmt.h"

#include <unordered_map>
#include <unordered_set>

namespace anta
{
	class LowerMFunc : public Replacer
	{
		Function& callerFn_;
	public:
		LowerMFunc(SemaFactory& f, Function& fn) : Replacer(f), callerFn_(fn) {}

		void run()
		{
			callerFn_.body_->postorder(*this);
			replace(callerFn_.body_);
		}

		using Replacer::visit;
		virtual void visit(MFuncCallStmt* caller) override;
		virtual void visit(MReturnStmt* stmt) override;
	};
}