/*
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_options.h"

#include "audit_event.h"
#include "audit_manager.h"
#include "audit_manager_global.h"
#include "mongo/base/init.h"
#include "mongo/db/server_options.h"
#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace audit {

    MONGO_MODULE_STARTUP_OPTIONS_REGISTER(AuditOptions)(InitializerContext* context) {
        return addAuditOptions(&moe::startupOptions);
    }

    MONGO_STARTUP_OPTIONS_STORE(AuditOptions)(InitializerContext* context) {
        return storeAuditOptions(moe::startupOptionsParsed, context->args());
    }

    MONGO_INITIALIZER_WITH_PREREQUISITES(InitializeGlobalAuditManager, ("CreateAuditManager"))
                                        (InitializerContext* context) {
        audit::getGlobalAuditManager()->enabled = auditGlobalParams.enabled;

        if (auditGlobalParams.enabled) {
            StatusWithMatchExpression parseResult =
                MatchExpressionParser::parse(auditGlobalParams.auditFilter);
            if (!parseResult.isOK()) {
                return Status(ErrorCodes::BadValue, "failed to parse auditFilter");
            }
            AuditManager* am = audit::getGlobalAuditManager();
            am->auditFilter = parseResult.getValue();

            am->auditLogPath = auditGlobalParams.auditPath;

            am->auditFormat = auditGlobalParams.auditFormat;
        }

        return Status::OK();
    }
} // namespace audit
} // namespace mongo
