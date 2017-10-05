#pragma once
#include <vector>
#include "Sema.h"
#include "Expr.h"
#include "Visitor.h"

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace anta
{
	// Forward declarations for the codegen
	struct StmtGenResult;
	struct EnvCodeGenCtx;

	using namespace boost;

    struct Statement : public SemanticNode, public intrusive_ref_counter<Statement, thread_unsafe_counter>
    {
        virtual StmtGenResult generate(EnvCodeGenCtx& ectx) = 0;

		virtual void visit(AbstractVisitor& v) = 0;
		virtual void preorder(AbstractVisitor& v) = 0;
		virtual void postorder(AbstractVisitor& v) = 0;

		virtual const Statement* last() const { return this; }

		void generateKill(EnvCodeGenCtx& ectx);
	};

	struct NoopStmt : public Statement
	{
		friend class Cloner; friend class Replacer;
	public:
		NoopStmt() {}

		virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override;
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};
	
	struct Statements : public Statement
    {
		friend class Cloner; friend class Replacer;
	private:
        std::vector<intrusive_ptr<Statement>> stmts_;
    public:
		Statements(std::vector<intrusive_ptr<Statement>> stmts);

		auto& stmts() { return stmts_; }

        virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override;
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;

		virtual const Statement* last() const override;
	};

    struct IfStmt : public Statement
    {
		friend class Cloner; friend class Replacer;
	private:
        intrusive_ptr<const Expr>		condition_;
		intrusive_ptr<Statement>			then_;
		intrusive_ptr<Statement>			else_;
    public:
		IfStmt(const intrusive_ptr<const Expr>& condition, const intrusive_ptr<Statement>& then, const intrusive_ptr<Statement>& else__);

		auto& condition() { return condition_; }
		auto& then() { return then_; }
		auto& elseB() { return else_; }

        virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override; 
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
    };


	struct FuncCallStmt : public Statement
    {
		friend class Cloner; friend class Replacer;
	private:
        const Function*           function_;
        std::vector<intrusive_ptr<const Expr>>  arguments_;
    public:
		FuncCallStmt(const Function* function, std::vector<intrusive_ptr<const Expr>> args);

		auto function() { return function_; }
		auto& arguments() { return arguments_; }

        virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override;
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};

    struct AssignStmt : public Statement
    {
		friend class Cloner; friend class Replacer; friend class LinearTypeWalker;
    private:
        intrusive_ptr<const Expr>               lhs_; // must evaluate to VarExpr
        intrusive_ptr<const Expr>               rhs_;
    public:
		AssignStmt(const intrusive_ptr<const Expr>& lhs, const intrusive_ptr<const Expr>& rhs);

		auto& lhs() { return lhs_; }
		auto& rhs() { return rhs_; }

        virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override; 
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};

    struct ReturnStmt : public Statement
    {
		friend class Cloner; friend class Replacer;
    private:
        intrusive_ptr<const Expr>               return_;
    public:
		ReturnStmt(const intrusive_ptr<const Expr>& expr);

		auto& _return() { return return_; }

        virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override; 
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};

    struct BreakStmt : public Statement
    {
	private:
		// how many levels (blocks) to break out
		unsigned level_;
	public:
		BreakStmt(int level) : level_(level) {}

		auto level() const { return level_; }
		auto& level() { return level_; }

		bool valid() const { return level_ != 0xffffffff; }

        virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override; 
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};

    struct ContinueStmt : public Statement
    {
	public:
		ContinueStmt() {}

		virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override; 
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};
    
	struct SwitchStmt : public Statement
	{
		friend class Cloner; friend class Replacer;
	private:
		intrusive_ptr<const Expr>				expr_;
		std::vector<intrusive_ptr<Statement>> branches_;
	public:
		SwitchStmt(const intrusive_ptr<const Expr>& expr, std::vector<intrusive_ptr<Statement>> branches);

		auto& expr() { return expr_; }
		auto& branches() { return branches_; }

		virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override; 
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};

	struct FuncletDef
	{
		std::string        label_;
		std::vector<intrusive_ptr<const Expr>> returnExprs_;
		intrusive_ptr<Statement>         stmt_;
	};

	struct MFuncCallStmt : public Statement
	{
		friend class Cloner; friend class Replacer; friend class LinearTypeWalker;
	private:
		const Function*           function_;
		std::vector<intrusive_ptr<const Expr>>  arguments_;
		std::vector<FuncletDef>   branches_;
	public:
		MFuncCallStmt(const Function* function, std::vector<intrusive_ptr<const Expr>> args, std::vector<FuncletDef> branches);

		auto function() { return function_; }
		auto& arguments() { return arguments_; }
		auto& branches() { return branches_; }

		virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override;
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};

	struct FuncletRet
	{
		std::string        label_;
		std::vector<intrusive_ptr<const Expr>> returnExprs_;
	};

	struct MReturnStmt : public Statement
	{
		friend class Cloner; friend class Replacer;
	private:
		FuncletRet return_;
	public:
		MReturnStmt(FuncletRet return_) : return_(std::move(return_)) {}
		auto& returnTarget() { return return_; }

		virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override;
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};

	struct BlockStmt : public Statement
	{
		friend class Cloner; friend class Replacer;
	private:
		intrusive_ptr<Statement> body_;
		bool loop_;
	public:
		BlockStmt(const intrusive_ptr<Statement>& body, bool loop);

		auto& body() { return body_; }
		auto loop() const { return loop_; }
		auto& loop() { return loop_; }

		virtual StmtGenResult generate(EnvCodeGenCtx& ectx) override;

		virtual void visit(AbstractVisitor& v) override;
		virtual void preorder(AbstractVisitor& v) override;
		virtual void postorder(AbstractVisitor& v) override;
	};
}

