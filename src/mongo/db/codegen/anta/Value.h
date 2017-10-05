#pragma once

#include "Type.h"

#include <boost/variant.hpp>
#include <string>
#include <vector>

namespace anta
{
	class Function;

    struct Value
    {
		using ArrayValue = std::vector<Value>;

		//                    0      1       2           3         4           5
        const boost::variant<int, int64_t, double, std::string, Function*, ArrayValue> value_;
        const Type*                                          type_;

        Value(const Type* type) : type_(type) {}

        template<typename T>
		Value(const Type* type, T v) : type_(type), value_(v) {}

        const Type* type() const { return type_; }

		const bool hasString() const { return value_.which() == 3; }

        template<typename T>
        const T& value() const
        {
            return boost::get<T>(value_);
        }
    };
}