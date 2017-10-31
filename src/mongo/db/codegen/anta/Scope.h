#pragma once
#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/Function.h"
#include "mongo/db/codegen/anta/Type.h"

#include <map>
#include <string>
#include <vector>

namespace anta
{
    class Scope
    {
        std::string                      name_;
	public:

        // functions declared at this scope
        std::map<std::string, std::unique_ptr<Function>> functions_;

        // types declared at this scope
        std::map<std::string, std::unique_ptr<Type>>     types_;

        // variables declared at this scope
        std::map<std::string, std::unique_ptr<Variable>> variables_;

		// a back link to the parent
		Scope*                                           parent_;

		// all nested scopes
        std::vector<std::unique_ptr<Scope>>              children_;

		Scope(std::string name, Scope* parent = nullptr) : name_(std::move(name)), parent_(parent)
        {
        }

		void addChild(std::unique_ptr<Scope>&& child)
		{
			children_.emplace_back(std::move(child));
		}

		Scope* parent() const { return parent_; }

		std::string name() const { return name_; }

        std::string getFullName() const
        {
            if (parent_)
                return parent_->getFullName().append(".").append(name_);
            else
                return name_;
        }
		Type* addType(const std::string& name, std::unique_ptr<Type>&& type);
        Type* getType(const std::string& name) const;

		Function* addFunction(const std::string& name, std::unique_ptr<Function>&& fn);
        Function* getFunction(const std::string& name) const;

		Variable* addVariable(const std::string& name, std::unique_ptr<Variable>&& v);
        Variable* getVariable(const std::string& name) const;
	};
}