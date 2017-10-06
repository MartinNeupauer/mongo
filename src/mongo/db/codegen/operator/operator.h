#pragma once

#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/Generator.h"

#include <string>
#include <memory>
#include <vector>

namespace rohan
{
	static constexpr const char* kOpenDone = "openDone";
	static constexpr const char* kError = "error";
	static constexpr const char* kCancel = "cancel";
	static constexpr const char* kNextRowDone = "nextRowDone";
	static constexpr const char* kNextRowEOS = "nextRowEOS";

	using SchemaType = std::vector<const anta::Type*>;

	struct GenContext
	{
		anta::Function* open_;
		anta::Function* nextrow_;

		bool disableInline_;

		GenContext() = delete;
		GenContext(bool disableInline) : disableInline_(disableInline) {}
	};

	class XteOperator : public anta::Generator
	{
		static unsigned nameCounter_;

	protected:

		std::string uniqueName(const std::string& name);

		void generateOpenParameters();
		void generateOpenReturns();

		void generateNextParameters();
		void generateNextReturns();

		void generateHandleErrorCancel(const anta::Wrapper& errorCode);

		std::vector<anta::Wrapper> generateColumnVariables(const SchemaType& schema, const char* colname = "column", anta::ExprPlaceholders* env = nullptr);

		void inlineOpen(GenContext&, GenContext&);
		void inlineGetNext(GenContext&, GenContext&);

		unsigned stateOffset_;
		SchemaType outputSchema_;

	public:
		XteOperator(anta::SemaFactory& f, const SchemaType& outputSchema) : anta::Generator(f), stateOffset_(0), outputSchema_(outputSchema) {}

		const SchemaType& outputSchema() const { return outputSchema_; }
		virtual void generate(GenContext&) = 0;
		virtual void calculateStateOffset(unsigned&) = 0;
	};
}