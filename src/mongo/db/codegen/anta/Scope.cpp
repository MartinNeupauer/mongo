#include "mongo/db/codegen/anta/Scope.h"

namespace anta
{

    Type* Scope::addType(const std::string& name, std::unique_ptr<Type>&& type)
    {
		if (type->scope() != this)
		{
			throw std::logic_error("invalid scope for type name " + name);
		}

		auto it = types_.emplace( name, std::move(type) );
        if (!it.second)
        {
            throw std::logic_error("duplicate type name " + name);
        }

		return it.first->second.get();
	}

    Type* Scope::getType(const std::string& name) const
    {
        auto i = types_.find(name);

        if (i == types_.end())
        {
            if (!parent_)
                return nullptr;

            return parent_->getType(name);
        }

        return i->second.get();
    }

	Function* Scope::addFunction(const std::string& name, std::unique_ptr<Function>&& fn)
    {
		if (fn->scope() != this)
		{
			throw std::logic_error("invalid scope for function name " + name);
		}

		auto it = functions_.emplace( name, std::move(fn) );
        if (!it.second)
        {
            throw std::logic_error("duplicate function name " + name);
        }

		return it.first->second.get();
	}

    Function* Scope::getFunction(const std::string& name) const
    {
        auto i = functions_.find(name);

        if (i == functions_.end())
        {
            if (!parent_)
                return nullptr;

            return parent_->getFunction(name);
        }

        return i->second.get();
    }

	Variable* Scope::addVariable(const std::string& name, std::unique_ptr<Variable>&& v)
    {
		if (v->scope() != this)
		{
			throw std::logic_error("invalid scope for variable name " + name);
		}

		auto it = variables_.emplace( name, std::move(v) );
        if (!it.second)
        {
            throw std::logic_error("duplicate variable name " + name);
        }

		return it.first->second.get();
    }

    Variable* Scope::getVariable(const std::string& name) const
    {
        auto i = variables_.find(name);

        if (i == variables_.end())
        {
            if (!parent_)
                return nullptr;

            return parent_->getVariable(name);
        }

        return i->second.get();
    }
}