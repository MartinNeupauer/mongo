/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

namespace audit {
namespace {

class ApplicationMessageEvent : public AuditEvent {
public:
    ApplicationMessageEvent(const AuditEventEnvelope& envelope, StringData msg)
        : AuditEvent(envelope), _msg(msg) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("msg", _msg);
        return builder;
    }

    const StringData _msg;
};

}  // namespace
}  // namespace audit

void audit::logApplicationMessage(Client* client, StringData msg) {
    if (!getGlobalAuditManager()->enabled)
        return;

    ApplicationMessageEvent event(
        makeEnvelope(client, ActionType::applicationMessage, ErrorCodes::OK), msg);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace mongo