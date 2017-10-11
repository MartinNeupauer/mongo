#include "mongo/db/codegen/operator/project.h"

namespace rohan
{
	void XteProject::generate(GenContext& ctx)
	{
		GenContext inputctx(ctx);
		input_->generate(inputctx);

		ctx.open_ = mfunction_(uniqueName("projectOpen"));
			generateOpenParameters();
			generateOpenReturns();
		{
			body_();
				auto errorCode = var_(int_, "%errorCode");

				// open an input
				mcall_(inputctx.open_, { var_("%state"), var_("%reopen") });
					branch_(rohan::kOpenDone);
						mreturn_(rohan::kOpenDone);
					end_();
					generateHandleErrorCancel(errorCode);
				end_();
			end_();

			inlineOpen(ctx,inputctx);
		}

		ctx.nextrow_ = mfunction_(uniqueName("projectNextRow"));
			generateNextParameters();
			generateNextReturns();
		{
			body_();
				auto errorCode = var_(int_, "%errorCode");
				auto inputColumns = generateColumnVariables(input_->outputSchema(), "inColumn", envIn_);
				auto outputColumns = generateColumnVariables(outputSchema(), "outColumn", envOut_);

				mcall_(inputctx.nextrow_, { var_("%state") });
					branch_(rohan::kNextRowDone, inputColumns);
					end_();
					branch_(rohan::kNextRowEOS);
						mreturn_(rohan::kNextRowEOS);
					end_();
					generateHandleErrorCancel(errorCode);
				end_();

				paste_(stmt_);

				mreturn_(rohan::kNextRowDone, outputColumns);
			end_();

			inlineGetNext(ctx, inputctx);
		}
	}

	void XteProject::calculateRuntimeState(RuntimeState& state)
	{
		// Project does not have any state for now so simply pass the call to the input
		input_->calculateRuntimeState(state);
	}

}