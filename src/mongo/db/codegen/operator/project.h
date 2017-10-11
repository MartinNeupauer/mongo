#pragma once

#include "mongo/db/codegen/operator/operator.h"

namespace rohan
{
	class XteProject : public XteOperator
	{
		std::unique_ptr<XteOperator> input_;

		anta::ExprPlaceholders* envIn_;
		anta::ExprPlaceholders* envOut_;
		boost::intrusive_ptr<anta::Statement> stmt_;

	public:
		XteProject(anta::SemaFactory& f, std::unique_ptr<XteOperator>&& input,
			anta::ExprPlaceholders* envIn, anta::ExprPlaceholders* envOut, boost::intrusive_ptr<anta::Statement> stmt)
			: XteOperator(f, envOut->types())
			, input_(std::move(input)) 
			, envIn_(envIn)
			, envOut_(envOut)
			, stmt_(stmt)
		{}

		virtual void generate(GenContext&) override;
		virtual void calculateRuntimeState(RuntimeState&) override;
	};
}