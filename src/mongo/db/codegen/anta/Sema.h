#pragma once

namespace anta
{
    // forward declarations

    struct SemanticNode;                          // a root node for all semantic constructs

    struct Statement;                         // an abstract base class for all statements
    struct Statements;                        // a sequence of statements
    struct IfStmt;                            // if (<cond expr) <stmts> else <stmts>
    struct FuncCallStmt;                      // void func(<expr>, <expr>, ...)
    struct AssignStmt;                        // <var expr> = <expr>
    struct ReturnStmt;                        // return <expr>
    struct BreakStmt;
    struct ContinueStmt;
	struct SwitchStmt;
	struct MFuncCallStmt;
	struct MReturnStmt;
	struct BlockStmt;
	struct NoopStmt;

    class Expr;                              // an abstract base class for all expressions
    class UnaryExpr;                         // !, ~, ...
    class BinaryExpr;                        // +, -, ...
    class VarExpr;                           // variable
    class StructAccessExpr;                  // (ptr)->member
    class FuncCallExpr;                      // a function call returning a value
    class ConstExpr;                         // a constant expression
	class DereferenceExpr;                   // *(ptr)
	class PointerArithmExpr;                 // a pointer arithmetic expression
	class BitCastExpr;
	class AddressOfExpr;
	class HoleExpr;
    class InlinedFuncExpr;
    
    class Type;                              // an abstract base class for a simple type system
    class VoidType;
	class Int1Type;
	class Int8Type;
	class Int16Type;
	class Int64Type;
    class Int128Type;
    class IntType;                           // temp
    class DoubleType;
    class StringType;
    class StructType;
    class PointerType;
	class ArrayType;

    class Function;
    class Variable;

    class Scope;
    struct Value;                             // a variant holding a value

    struct CodeGenContext;
    struct EnvCodeGenCtx;

    struct SemaFactory;

	class AbstractVisitor;

    struct SemanticNode
    {
        virtual ~SemanticNode() = 0;
	
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

	inline SemanticNode::~SemanticNode() {}

	inline unsigned alignUp(unsigned x, unsigned a) { return (x + (a - 1)) & ~(a - 1); }

}

