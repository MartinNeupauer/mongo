#pragma once

#include "mongo/db/codegen/anta/Sema.h"
#include <string>
#include <utility>

namespace anta
{
    class Variable
    {
		const Scope*       scope_;
        const Type*        type_;
        const std::string  name_;
		const bool         ssa_;
    public:
        Variable(const Scope* scope, const Type* type, std::string name, bool ssa = false) 
			: scope_(scope), type_(type), name_(std::move(name)), ssa_(ssa) 
		{
		}
		const Scope* scope() const { return scope_; }
		const std::string& name() const { return name_; }
        const Type* type() const { return type_; }
		bool isssa() const { return ssa_; }
        bool operator == (const Variable& other) const
        {
            return scope_ == other.scope_ && type_ == other.type_ && name_ == other.name_ && ssa_ == other.ssa_;
        }
    };
}
