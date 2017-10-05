#pragma once
#include "Factory.h"
#include "Sema.h"
#include "ReplaceStmt.h"

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
/*			auto it = stmtMap_.find(callerFn_.body_.get());
			if (it != stmtMap_.end())
				callerFn_.body_ = it->second;
*/		}

		virtual void visit(MFuncCallStmt* caller) override;
		virtual void visit(MReturnStmt* stmt) override;
	};
}