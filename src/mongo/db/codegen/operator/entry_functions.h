#pragma once

#include "mongo/db/codegen/operator/operator.h"

#include <string>

namespace rohan
{
	enum class OpenCallResult : int
	{
		kDone = 0,
		kError = -1,
		kCancel = -2
	};

	enum class GetNextCallResult : int
	{
		kOK = 0,
		kEOS = -1,
		kError = -2,
		kCancel = -3
	};

	using NativeOpenFunction = OpenCallResult(*)(char*, int*);
	using NativeGetNextFunction = GetNextCallResult(*)(char*, int*);

	class EntryFunctions : public anta::Generator
	{
	public:
		EntryFunctions(anta::SemaFactory& f) : anta::Generator(f) {}

		anta::Function* generateOpen(GenContext& entry);
		anta::Function* generateGetNext(GenContext& entry);
	};
}