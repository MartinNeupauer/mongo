/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include "mongo/base/disallow_copying.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/oid.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/principal_set.h"
#include "mongo/db/jsobj.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/net/sock.h"

namespace mongo {
namespace audit {

    class AuditOperationId {
    public:
        AuditOperationId() : _connectionId(), _operationNumber(0) {}
        AuditOperationId(OID connectionId, int64_t operationNumber) :
            _connectionId(connectionId),
            _operationNumber(operationNumber) {}

        const OID& getConnectionId() const { return _connectionId; }
        int64_t getOperationNumber() const { return _operationNumber; }

    private:
        OID _connectionId;
        int64_t _operationNumber;
    };

    struct AuditEventEnvelope {
        Date_t timestamp;
        AuditOperationId opId;
        SockAddr localAddr;
        SockAddr remoteAddr;
        PrincipalSet::NameIterator authenticatedUsers;
        ActionType actionType;
        ErrorCodes::Error result;
    };

    /**
     * Base class of types representing events for writing to the audit log.
     *
     * Instances of subclasses of AuditEvent will typically be ephemeral, and built on the stack for
     * immediate writing into the audit log domain.  They are not intended to be stored, and may not
     * own all of the data they refernence.
     */
    class AuditEvent {
        MONGO_DISALLOW_COPYING(AuditEvent);
    public:
        Date_t getTimestamp() const { return _envelope.timestamp; }
        const AuditOperationId& getOperationId() const { return _envelope.opId; }
        const PrincipalSet::NameIterator& getAuthenticatedUsers() const {
            return _envelope.authenticatedUsers;
        }

        const SockAddr& getLocalAddr() const { return _envelope.localAddr; }
        const SockAddr& getRemoteAddr() const { return _envelope.remoteAddr; }
        ActionType getActionType() const { return _envelope.actionType; }
        ErrorCodes::Error getResultCode() const { return _envelope.result; }

        /**
         * Puts human-readable description of this event into "os".
         */
        std::ostream& putText(std::ostream& os) const { return putTextDescription(os); }

        /**
         * Builds BSON describing this event into "builder".
         */
        BSONObjBuilder& putBSON(BSONObjBuilder& builder) const;

    protected:
        explicit AuditEvent(const AuditEventEnvelope& envelope) : _envelope(envelope) {}

        /**
         * Destructor.  NOT virtual.  Do not attempt to delete a pointer to AuditEvent.
         */
        ~AuditEvent() {}

    private:
        virtual std::ostream& putTextDescription(std::ostream& os) const = 0;
        virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const = 0;

        AuditEventEnvelope _envelope;
    };

}  // namespace audit
}  // namespace mongo
