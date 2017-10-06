#include "mongo/db/codegen/operator/operator.h"

namespace rohan
{
	unsigned XteOperator::nameCounter_ = 1;

	std::string XteOperator::uniqueName(const std::string & name)
	{
		return name + std::to_string(nameCounter_++);
	}
	void XteOperator::generateOpenParameters()
	{
		param_(pint8_, "%state");
		param_(int1_, "%reopen");
		// set of correlated parameters?
	}
	void XteOperator::generateOpenReturns()
	{
		return_(kOpenDone, {});
		return_(kError, {int_/*error code*/});
		return_(kCancel, {});
	}
	void XteOperator::generateNextParameters()
	{
		param_(ptr_(int8_), "%state");
	}

	std::vector<anta::Wrapper> XteOperator::generateColumnVariables(const SchemaType& schema, const char* colname, anta::ExprPlaceholders* env)
	{
		std::vector<anta::Wrapper> results;
		int counter = 0;
		for (auto t : schema)
		{
			std::string name(colname); name.append(std::to_string(counter));
			results.emplace_back(var_(t, name.c_str()));

			if (env)
			{
				env->fillHole(counter, results.back().expr_);
			}
			++counter;
		}
		return results;
	}

	void XteOperator::inlineOpen(GenContext& ctx, GenContext& inputctx)
	{
		if (!ctx.disableInline_)
		{
			anta::InlineMFunc inliner(factory_, *ctx.open_, inputctx.open_);
			inliner.run();
			factory_.destroyFunction(inputctx.open_);
		}
	}

	void XteOperator::inlineGetNext(GenContext& ctx, GenContext& inputctx)
	{
		if (!ctx.disableInline_)
		{
			anta::InlineMFunc inliner(factory_, *ctx.nextrow_, inputctx.nextrow_);
			inliner.run();
			factory_.destroyFunction(inputctx.nextrow_);
		}
	}

	void XteOperator::generateNextReturns()
	{
		return_(kNextRowDone, outputSchema_);
		return_(kNextRowEOS, {/*statistics?*/});
		return_(kError, { int_/*error code*/ });
		return_(kCancel, {});
	}
	void XteOperator::generateHandleErrorCancel(const anta::Wrapper & errorCode)
	{
		branch_(kError, { errorCode });
			mreturn_(kError, {errorCode});
		end_();
		branch_(kCancel);
			mreturn_(kCancel);
		end_();
	}
}