/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/commands.h"

#include "../query_analysis/query_analysis.h"

namespace mongo {
namespace {


/**
 * NOTE: The only method called is run(), the rest exist simply to ensure the code compiles.
 */
class CryptDPlaceholder : public BasicCommand {
public:
    CryptDPlaceholder(StringData name, StringData oldName = StringData())
        : BasicCommand(name, oldName) {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        MONGO_UNREACHABLE;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        MONGO_UNREACHABLE;
    }

    Status explain(OperationContext* opCtx,
                   const OpMsgRequest& request,
                   ExplainOptions::Verbosity verbosity,
                   rpc::ReplyBuilderInterface* result) const final {

        try {
            BSONObjBuilder builder = result->getBodyBuilder();
            processCommand(request.body, &builder);
        } catch (...) {
            return exceptionToStatus();
        }

        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& jsobj,
             BSONObjBuilder& result) final {

        processCommand(jsobj, &result);

        return true;
    }

    virtual void processCommand(const BSONObj& jsobj, BSONObjBuilder* result) const = 0;
};


class CryptDFind final : public CryptDPlaceholder {
public:
    CryptDFind() : CryptDPlaceholder("find") {}

    void processCommand(const BSONObj& jsobj, BSONObjBuilder* result) const final {
        processFindCommand(jsobj, result);
    }
} cmdCryptDFind;


class CryptDAggregate final : public CryptDPlaceholder {
public:
    CryptDAggregate() : CryptDPlaceholder("aggregate") {}

    void processCommand(const BSONObj& jsobj, BSONObjBuilder* result) const final {
        processAggregateCommand(jsobj, result);
    }
} cmdCryptDAggregate;


class CryptDDistinct final : public CryptDPlaceholder {
public:
    CryptDDistinct() : CryptDPlaceholder("distinct") {}

    void processCommand(const BSONObj& jsobj, BSONObjBuilder* result) const final {
        processDistinctCommand(jsobj, result);
    }
} cmdCryptDDistinct;

class CryptDCount final : public CryptDPlaceholder {
public:
    CryptDCount() : CryptDPlaceholder("count") {}

    void processCommand(const BSONObj& jsobj, BSONObjBuilder* result) const final {
        processCountCommand(jsobj, result);
    }
} cmdCryptDCount;


class CryptDFindAndModify final : public CryptDPlaceholder {
public:
    CryptDFindAndModify() : CryptDPlaceholder("findAndModify", "findandmodify") {}

    void processCommand(const BSONObj& jsobj, BSONObjBuilder* result) const final {
        processFindAndModifyCommand(jsobj, result);
    }
} cmdCryptDFindAndModify;


/**
 * A command for running write ops.
 */
class CryptDWriteOp : public Command {
public:
    CryptDWriteOp(StringData name) : Command(name) {}

    AllowedOnSecondary secondaryAllowed(ServiceContext* context) const final {
        MONGO_UNREACHABLE;
    }

    bool maintenanceOk() const final {
        MONGO_UNREACHABLE;
    }

    bool adminOnly() const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        MONGO_UNREACHABLE;
    }

    LogicalOp getLogicalOp() const final {
        MONGO_UNREACHABLE;
    }

    ReadWriteType getReadWriteType() const final {
        MONGO_UNREACHABLE;
    }

    std::size_t reserveBytesForReply() const final {
        MONGO_UNREACHABLE;
    }

    bool shouldAffectCommandCounter() const final {
        MONGO_UNREACHABLE;
    }

    class InvocationBase : public CommandInvocation {
    public:
        InvocationBase(const CryptDWriteOp* definition,
                       const OpMsgRequest& request,
                       StringData dbName)
            : CommandInvocation(definition), _request(request), _dbName(dbName) {}

    private:
        bool supportsWriteConcern() const final {
            MONGO_UNREACHABLE;
        }

        bool supportsReadConcern(repl::ReadConcernLevel level) const final {
            MONGO_UNREACHABLE;
        }

        bool allowsSpeculativeMajorityReads() const final {
            MONGO_UNREACHABLE;
        }

        NamespaceString ns() const final {
            return NamespaceString(CommandHelpers::parseNsFromCommand(_dbName, _request.body));
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {}

        void explain(OperationContext* opCtx,
                     ExplainOptions::Verbosity verbosity,
                     rpc::ReplyBuilderInterface* result) final {

            auto builder = result->getBodyBuilder();

            processWriteCommand(_request, &builder);
        }


        void run(OperationContext* opCtx, rpc::ReplyBuilderInterface* result) final {

            auto builder = result->getBodyBuilder();

            processWriteCommand(_request, &builder);
        }


        virtual void processWriteCommand(const OpMsgRequest& request, BSONObjBuilder* builder) = 0;


    private:
        const OpMsgRequest& _request;
        const StringData _dbName;
    };
};


class CryptDInsertCmd final : public CryptDWriteOp {
public:
    CryptDInsertCmd() : CryptDWriteOp("insert") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) final {
        return std::make_unique<Invocation>(this, opMsgRequest, opMsgRequest.getDatabase());
    }

    class Invocation : public CryptDWriteOp::InvocationBase {
    public:
        Invocation(const CryptDInsertCmd* definition,
                   const OpMsgRequest& request,
                   StringData dbName)
            : InvocationBase(definition, request, dbName) {}


        void processWriteCommand(const OpMsgRequest& request, BSONObjBuilder* builder) final {
            processInsertCommand(request, builder);
        }
    };

} insertCmd;


class CryptDUpdateCmd final : public CryptDWriteOp {
public:
    CryptDUpdateCmd() : CryptDWriteOp("update") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) final {
        return std::make_unique<Invocation>(this, opMsgRequest, opMsgRequest.getDatabase());
    }

    class Invocation : public CryptDWriteOp::InvocationBase {
    public:
        Invocation(const CryptDUpdateCmd* definition,
                   const OpMsgRequest& request,
                   StringData dbName)
            : InvocationBase(definition, request, dbName) {}


        void processWriteCommand(const OpMsgRequest& request, BSONObjBuilder* builder) final {
            processUpdateCommand(request, builder);
        }
    };

} updateCmd;


class CryptDDeleteCmd final : public CryptDWriteOp {
public:
    CryptDDeleteCmd() : CryptDWriteOp("delete") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) final {
        return std::make_unique<Invocation>(this, opMsgRequest, opMsgRequest.getDatabase());
    }

    class Invocation : public CryptDWriteOp::InvocationBase {
    public:
        Invocation(const CryptDDeleteCmd* definition,
                   const OpMsgRequest& request,
                   StringData dbName)
            : InvocationBase(definition, request, dbName) {}


        void processWriteCommand(const OpMsgRequest& request, BSONObjBuilder* builder) final {
            processDeleteCommand(request, builder);
        }
    };

} deleteCmd;


/**
 * The explain command in mongod checks the replication coordinator and so cryptd uses its own
 * version of explain.
 */
class CryptdExplainCmd final : public Command {
public:
    CryptdExplainCmd() : Command("explain") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& request) override;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kOptIn;
    }

    bool maintenanceOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return false;
    }

    std::string help() const override {
        return "explain database reads and writes";
    }

private:
    class Invocation;
} cryptdExplainCmd;

class CryptdExplainCmd::Invocation final : public CommandInvocation {
public:
    Invocation(const CryptdExplainCmd* explainCommand,
               const OpMsgRequest& request,
               ExplainOptions::Verbosity verbosity,
               std::unique_ptr<OpMsgRequest> innerRequest,
               std::unique_ptr<CommandInvocation> innerInvocation)
        : CommandInvocation(explainCommand),
          _outerRequest{&request},
          _dbName{_outerRequest->getDatabase().toString()},
          _ns{CommandHelpers::parseNsFromCommand(_dbName, _outerRequest->body)},
          _verbosity{std::move(verbosity)},
          _innerRequest{std::move(innerRequest)},
          _innerInvocation{std::move(innerInvocation)} {}

private:
    void run(OperationContext* opCtx, rpc::ReplyBuilderInterface* result) override {
        _innerInvocation->explain(opCtx, _verbosity, result);
    }

    void explain(OperationContext* opCtx,
                 ExplainOptions::Verbosity verbosity,
                 rpc::ReplyBuilderInterface* result) override {
        uasserted(ErrorCodes::IllegalOperation, "Explain cannot explain itself.");
    }

    NamespaceString ns() const override {
        return _ns;
    }

    bool supportsWriteConcern() const override {
        return false;
    }

    /**
     * You are authorized to run an explain if you are authorized to run
     * the command that you are explaining. The auth check is performed recursively
     * on the nested command.
     */
    void doCheckAuthorization(OperationContext* opCtx) const override {
        _innerInvocation->checkAuthorization(opCtx, *_innerRequest);
    }

    const CryptdExplainCmd* command() const {
        return static_cast<const CryptdExplainCmd*>(definition());
    }

    const OpMsgRequest* _outerRequest;
    const std::string _dbName;
    NamespaceString _ns;
    ExplainOptions::Verbosity _verbosity;
    std::unique_ptr<OpMsgRequest> _innerRequest;  // Lifespan must enclose that of _innerInvocation.
    std::unique_ptr<CommandInvocation> _innerInvocation;
};

std::unique_ptr<CommandInvocation> CryptdExplainCmd::parse(OperationContext* opCtx,
                                                           const OpMsgRequest& request) {
    CommandHelpers::uassertNoDocumentSequences(getName(), request);

    std::string dbname = request.getDatabase().toString();
    const BSONObj& cmdObj = request.body;

    ExplainOptions::Verbosity verbosity = uassertStatusOK(ExplainOptions::parseCmdBSON(cmdObj));
    uassert(ErrorCodes::BadValue,
            "explain command requires a nested object",
            cmdObj.firstElement().type() == Object);

    auto explainedObj = cmdObj.firstElement().Obj();
    if (auto innerDb = explainedObj["$db"]) {
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << "Mismatched $db in explain command. Expected " << dbname
                              << " but got "
                              << innerDb.checkAndGetStringData(),
                innerDb.checkAndGetStringData() == dbname);
    }

    auto explainedCommand = CommandHelpers::findCommand(explainedObj.firstElementFieldName());
    uassert(ErrorCodes::CommandNotFound,
            str::stream() << "Explain failed due to unknown command: "
                          << explainedObj.firstElementFieldName(),
            explainedCommand);

    auto innerRequest =
        stdx::make_unique<OpMsgRequest>(OpMsgRequest::fromDBAndBody(dbname, explainedObj));

    auto innerInvocation = explainedCommand->parse(opCtx, *innerRequest);

    return stdx::make_unique<Invocation>(
        this, request, std::move(verbosity), std::move(innerRequest), std::move(innerInvocation));
}

}  // namespace
}  // namespace mongo
