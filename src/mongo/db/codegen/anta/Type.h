#pragma once
#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/CodeGenCtx.h"

#include <llvm/IR/DerivedTypes.h>

#include <string>
#include <vector>
#include <unordered_map>


namespace llvm
{
	class Function;
	class Value;
}

namespace anta
{
    class Type
    {
		// scope where the type is declared
		const Scope*      scope_;
        const std::string name_;
	protected:
		bool              linear_;
	public:
        Type(const Scope* scope, std::string name, bool linear=false) : scope_(scope), name_(std::move(name)), linear_(linear) {}
        virtual ~Type() = 0;

		virtual bool isInteger() const { return false; }
        virtual llvm::Type* getllvm(CodeGenContext&) const = 0;
		virtual void generateUndef(EnvCodeGenCtx&, llvm::Value*) const
		{
			throw std::logic_error("unsupported type");
		}
		virtual void generateCheckUndef(EnvCodeGenCtx&, llvm::Value*) const
		{
			throw std::logic_error("unsupported type");
		}
		virtual void generateKill(EnvCodeGenCtx&, llvm::Value*) const
		{
			throw std::logic_error("unsupported type");
		}
		virtual unsigned alignSize() const = 0;
		virtual unsigned allocSize() const = 0;
		virtual bool canCast(const Type*) const;

		const std::string& name() const
		{
			return name_;
		}

		const Scope* scope() const
		{
			return scope_;
		}

		bool linear() const
		{
			return linear_;
		}

		void setLinear()
		{
			linear_ = true;
		}

		template <typename T>
		T* is_a()
		{
			return dynamic_cast<T*>(this);
		}

		template <typename T>
		const T* is_a() const
		{
			return dynamic_cast<const T*>(this);
		}
	};

    inline Type::~Type() {}

	class Int1Type : public Type
	{
	public:
		Int1Type(const Scope* scope) : Type(scope, "int1") {}
		virtual bool isInteger() const override { return true; }

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
		{
			return llvm::Type::getInt1Ty(ctx.context_);
		}
		virtual unsigned alignSize() const override { return 1; }
		virtual unsigned allocSize() const override { return 1; }
	};

	class Int8Type : public Type
	{
	public:
		Int8Type(const Scope* scope) : Type(scope, "int8") {}
		virtual bool isInteger() const override { return true; }

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
		{
			return llvm::Type::getInt8Ty(ctx.context_);
		}
		virtual unsigned alignSize() const override { return 1; }
		virtual unsigned allocSize() const override { return 1; }
	};

	class Int16Type : public Type
	{
	public:
		Int16Type(const Scope* scope) : Type(scope, "int16") {}
		virtual bool isInteger() const override { return true; }

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
		{
			return llvm::Type::getInt16Ty(ctx.context_);
		}
		virtual unsigned alignSize() const override { return 2; }
		virtual unsigned allocSize() const override { return 2; }
	};

	class IntType : public Type
	{
	public:
		IntType(const Scope* scope) : Type(scope, "int") {}
		virtual bool isInteger() const override { return true; }

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
		{
			return llvm::Type::getInt32Ty(ctx.context_);
		}
		virtual unsigned alignSize() const override { return 4; }
		virtual unsigned allocSize() const override { return 4; }
	};

	class Int64Type : public Type
	{
	public:
		Int64Type(const Scope* scope) : Type(scope, "int64") {}
		virtual bool isInteger() const override { return true; }

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
		{
			return llvm::Type::getInt64Ty(ctx.context_);
		}
		virtual unsigned alignSize() const override { return 8; }
		virtual unsigned allocSize() const override { return 8; }
	};

	class DoubleType : public Type
    {
	public:
        DoubleType(const Scope* scope) : Type(scope, "double") {}
        virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
        {
            return llvm::Type::getDoubleTy(ctx.context_);
        }
		virtual unsigned alignSize() const override { return 8; }
		virtual unsigned allocSize() const override { return 8; }
	};

    class StructType : public Type
    {
        std::vector<const Type*>             types_;
		std::unordered_map<std::string, int> ordinals_;

		// UNDONE - this is not the right place ... it should go to CodeGenContext or something similar
		llvm::StructType*        m_type;
	public:
        StructType(const Scope* scope, const std::string& name) : Type(scope, name), m_type(nullptr) {}

		void addField(const Type* type, const std::string& name)
		{
			if (types_.size() != ordinals_.size())
				throw std::logic_error("cannot mix fields with and without names");

			types_.push_back(type);

			if (!ordinals_.insert({ name, types_.size() - 1 }).second)
				throw std::logic_error("duplicate field definition " + name);

			if (type->linear())
				linear_ = true;
		}

		void addFields(const std::vector<const Type*>& types)
		{
			if (!ordinals_.empty())
				throw std::logic_error("cannot mix fields with and without names");

			types_.insert(types_.end(), types.begin(), types.end());

			for (auto t : types)
				if (t->linear())
					linear_ = true;
		}

		auto fieldCount() const { return types_.size(); }

        virtual llvm::Type* getllvm(CodeGenContext&) const override
        {
			if (!m_type)
				throw std::logic_error("struct type not generated");

            return m_type;
        }

		virtual unsigned alignSize() const override 
		{
			unsigned alignment = 0;
			for (auto t : types_)
				alignment = std::max(alignment, t->alignSize());

			return alignment;
		}
		virtual unsigned allocSize() const override
		{
			unsigned size = 0;
			for (auto t : types_)
				size = alignUp(size, t->alignSize()) + t->allocSize();

			return alignUp(size, alignSize());
		}

		int ordinal(const std::string& field) const
		{
			auto it = ordinals_.find(field);
			if (it == ordinals_.end())
				throw std::logic_error("unknow field");

			return it->second;
		}

		const Type* fieldType(int ordinal) const
		{
			return types_[ordinal];
		}

		std::string fieldName(int ordinal) const
		{
			// ugly lienar search
			for (auto& f : ordinals_)
				if (f.second == ordinal)
					return f.first;

			return std::to_string(ordinal);
		}

		void generateDeclaration(CodeGenContext& ctx, const std::string& fullName)
		{
			if (m_type)
				throw std::logic_error("struct type already generated");

			if (!name().empty())
				m_type = llvm::StructType::create(ctx.context_, fullName);
		}

		void generateDefinition(CodeGenContext& ctx)
		{
			if (!name().empty() && !m_type)
				throw std::logic_error("struct type not generated");

			if (name().empty() && m_type)
				throw std::logic_error("struct type not generated");

			std::vector<llvm::Type*> fields;
			fields.reserve(types_.size());

			for (auto f : types_)
				fields.push_back(f->getllvm(ctx));

			if (name().empty())
			{
				m_type = llvm::StructType::get(ctx.context_, fields);
			}
			else
			{
				m_type->setBody(fields);
			}
		}

		virtual void generateUndef(EnvCodeGenCtx&, llvm::Value*) const override;
		virtual void generateCheckUndef(EnvCodeGenCtx&, llvm::Value*) const override
		{
			// struct value is always valid
		}
		virtual void generateKill(EnvCodeGenCtx&, llvm::Value*) const override;
    };

	class ArrayType : public Type
	{
	protected:
		const Type* arrayOf_;
		unsigned    size_;
	public:
		ArrayType(const Scope* scope, const Type* arrayOf, unsigned size, bool linear = false) : Type(scope, arrayOf->name() + "[]", linear), arrayOf_(arrayOf), size_(size)
		{
			if (!arrayOf_) throw std::logic_error("arrayOf must not be nullptr");
		}
		ArrayType(const Scope* scope, const Type* arrayOf, unsigned size, const std::string& name, bool linear = false) : Type(scope, name, linear), arrayOf_(arrayOf), size_(size)
		{
			if (!arrayOf_) throw std::logic_error("arrayOf must not be nullptr");
		}

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
		{
			return llvm::ArrayType::get(arrayOf_->getllvm(ctx), size_);
		}
		virtual unsigned alignSize() const override { return arrayOf_->alignSize(); }
		virtual unsigned allocSize() const override 
		{
			if (arrayOf_->allocSize() % arrayOf_->alignSize())
				throw std::logic_error("size mod align must be 0");

			return arrayOf_->allocSize() * size_;
		}
	};

    class PointerType : public Type
    {
        const Type* pointerOf_;
	public:
        PointerType(const Scope* scope, const Type* pointerOf, bool linear = false) : Type(scope, pointerOf->name() + "*", linear), pointerOf_(pointerOf)
		{
			if (!pointerOf_) throw std::logic_error("pointerOf must not be nullptr");
		}
		PointerType(const Scope* scope, const Type* pointerOf, const std::string& name, bool linear = false) : Type(scope, name, linear), pointerOf_(pointerOf)
		{
			if (!pointerOf_) throw std::logic_error("pointerOf must not be nullptr");
		}

		const Type* pointee() const
		{
			return pointerOf_;
		}

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
        {
            return llvm::PointerType::getUnqual(pointerOf_->getllvm(ctx));
        }
		virtual unsigned alignSize() const override { return 8; }
		virtual unsigned allocSize() const override { return 8; }
		virtual bool canCast(const Type* dest) const override;
	};

	class StringType : public PointerType
	{
	public:
		StringType(const Scope* scope, const Type* pointerOf, bool linear) : PointerType(scope, pointerOf, linear ? "String" : "StringView", linear) {}

		virtual bool canCast(const Type* dest) const override;

		virtual void generateUndef(EnvCodeGenCtx&, llvm::Value*) const override;
		virtual void generateCheckUndef(EnvCodeGenCtx&, llvm::Value*) const override;
		virtual void generateKill(EnvCodeGenCtx&, llvm::Value*) const override;
	};

	// The Variant type 
	class BSONVariant : public ArrayType
	{
	public:
		static const unsigned int kSize = 20;
/*
		static const unsigned int kTagOffset = 0;
		static const unsigned int kInlineStringOffset = 1;
		static const unsigned int kMemoryPtrOffset = 4;
		static const unsigned int kAllocatorPtrOffset = 12;
		static const unsigned int kScalarValueOffset = 4;
*/
		static const unsigned int kTagOffset = 19;
		static const unsigned int kInlineStringOffset = 0;
		static const unsigned int kMemoryPtrOffset = 0;
		static const unsigned int kAllocatorPtrOffset = 8;
		static const unsigned int kScalarValueOffset = 0;
		
		BSONVariant(const Scope* scope, const Type* storageType, bool linear) : ArrayType(scope, storageType, kSize, linear ? "BSONVariant" : "BSONVariantView", linear) {}

		virtual llvm::Type* getllvm(CodeGenContext& ctx) const override
		{
			return llvm::Type::getIntNTy(ctx.context_, 8 * kSize);
		}

		virtual bool canCast(const Type* dest) const override;

		virtual void generateUndef(EnvCodeGenCtx&, llvm::Value*) const override;
		virtual void generateCheckUndef(EnvCodeGenCtx&, llvm::Value*) const override;
		virtual void generateKill(EnvCodeGenCtx&, llvm::Value*) const override;
	};

	class FunctionType : public Type
	{
		Function* fn_;
	public:
		FunctionType(Function* fn);

		virtual llvm::Type* getllvm(CodeGenContext&) const override;
		virtual unsigned alignSize() const override
		{
			throw std::logic_error("unsupported type");
		}
		virtual unsigned allocSize() const override
		{
			throw std::logic_error("unsupported type");
		}
	};
}