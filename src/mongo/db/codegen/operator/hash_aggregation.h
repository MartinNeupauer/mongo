#pragma once

#include "mongo/db/codegen/operator/operator.h"

namespace rohan
{
	class XteHashAgg : public XteOperator
	{
		std::unique_ptr<XteOperator> input_;

		// group by columns
		std::vector<unsigned> groupby_;

		anta::ExprPlaceholders* envIn_;
		anta::ExprPlaceholders* envOut_;
		anta::ExprPlaceholders* envTableOpen_;
		anta::ExprPlaceholders* envTableGetRow_;
		boost::intrusive_ptr<anta::Statement> initGroupStmt_;
		boost::intrusive_ptr<anta::Statement> aggStepStmt_;
		boost::intrusive_ptr<anta::Statement> finalStepStmt_;

		// hash table iterator offset
		unsigned hashIteratorOffset_;
		// last keyval pointer
		unsigned hashLastKeyValOffset_;

		const anta::StructType* generateKeyStruct();
		const anta::StructType* generateKeyValueStruct();
		anta::Function* generateKeyHashFunction(const anta::Type* keyStruct);
		anta::Function* generateKeyCompareFunction(const anta::Type* keyStruct);
	public:
		XteHashAgg(anta::SemaFactory& f, std::unique_ptr<XteOperator>&& input,
			const std::vector<unsigned> groupby,
			anta::ExprPlaceholders* envIn,
			anta::ExprPlaceholders* envOut,
			anta::ExprPlaceholders* envTableOpen,
			anta::ExprPlaceholders* envTableGetRow,
			boost::intrusive_ptr<anta::Statement> initGroup,
			boost::intrusive_ptr<anta::Statement> aggStep,
			boost::intrusive_ptr<anta::Statement> finalStep
		)
			: XteOperator(f, envOut->types())
			, input_(std::move(input))
			, groupby_(groupby)
			, envIn_(envIn)
			, envOut_(envOut)
			, envTableOpen_(envTableOpen)
			, envTableGetRow_(envTableGetRow)
			, initGroupStmt_(initGroup)
			, aggStepStmt_(aggStep)
			, finalStepStmt_(finalStep)
			, hashIteratorOffset_(0)
			, hashLastKeyValOffset_(0)
		{}

		virtual void generate(rohan::GenContext&) override;
		virtual void calculateRuntimeState(RuntimeState&) override;
	};
}