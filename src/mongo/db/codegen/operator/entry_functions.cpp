#include "mongo/db/codegen/operator/entry_functions.h"

namespace rohan
{
	anta::Function * EntryFunctions::generateOpen(GenContext& entry)
	{
		auto fn = function_(int_, "entry_open");
			auto state    = param_(ptr_(int8_), "%state");
			auto outError = param_(ptr_(int_), "%outError");
		body_();
			auto errorCodeOpen = var_(int_, "%errorCodeOpen");

			mcall_(entry.open_, {state, const1_(false)});
				branch_(kOpenDone);
					return_(const_((int)OpenCallResult::kDone));
				end_();
				branch_(kError, { errorCodeOpen });
					*outError = errorCodeOpen;
					return_(const_((int)OpenCallResult::kError));
				end_();
				branch_(kCancel);
					return_(const_((int)OpenCallResult::kCancel));
				end_();
			end_();
		end_();	

		if (!entry.disableInline_)
		{
			anta::InlineMFunc inliner(factory_, *fn, { entry.open_ });
			inliner.run();
			factory_.destroyFunction(entry.open_);
		}
		return fn;
	}

	anta::Function* EntryFunctions::generateGetNext(GenContext& entry)
	{
		auto fn = function_(int_, "entry_getnext");
			auto state    = param_(ptr_(int8_), "%state");
			auto outError = param_(ptr_(int_), "%outError");
		body_();
			auto errorCodeNextRow = var_(int_, "%errorCodeNextRow");
			mcall_(entry.nextrow_, {state});
				branch_(kNextRowDone, {/*column1,column2,column3,column4,column5*/});
					return_(const_((int)GetNextCallResult::kOK));
				end_();
				branch_(kNextRowEOS);
					return_(const_((int)GetNextCallResult::kEOS));
				end_();
				branch_(kError, { errorCodeNextRow });
					*outError = errorCodeNextRow;
					return_(const_((int)GetNextCallResult::kError));
				end_();
				branch_(kCancel);
					return_(const_((int)GetNextCallResult::kCancel));
				end_();
			end_();
		end_();

		if (!entry.disableInline_)
		{
			anta::InlineMFunc inliner(factory_, *fn, { entry.nextrow_ });
			inliner.run();
			factory_.destroyFunction(entry.nextrow_);
		}
		return fn;
	}
}