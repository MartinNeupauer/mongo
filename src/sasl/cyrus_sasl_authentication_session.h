/*
 * Copyright (C) 2014 10gen, Inc.  All Rights Reserved.
 */

#pragma once

#include <sasl/sasl.h>
#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/sasl_authentication_session.h"
#include "mongo/platform/cstdint.h"

namespace mongo {
    
    /**
     * Authentication session data for the server side of SASL authentication.
     */
    class CyrusSaslAuthenticationSession : public SaslAuthenticationSession {
        MONGO_DISALLOW_COPYING(CyrusSaslAuthenticationSession);
    public:
        struct SaslMechanismInfo;

        static const int mongoSessionCallbackId;

        static Status smokeTestMechanism(StringData mechanism,
                                         StringData serviceName,
                                         StringData serviceHostname);

        explicit CyrusSaslAuthenticationSession(AuthorizationSession* authSession);
        virtual ~CyrusSaslAuthenticationSession();

        virtual Status start(StringData authenticationDatabase,
                             StringData mechanism,
                             StringData serviceName,
                             StringData serviceHostname,
                             int64_t conversationId,
                             bool autoAuthorize);

        virtual Status step(StringData inputData, std::string* outputData);

        virtual std::string getPrincipalId() const;
    
        virtual const char* getMechanism() const;

        /**
         * Returns a pointer to the opaque SaslMechanismInfo object for the mechanism in use.
         *
         * Not meaningful before a successful call to start().
         */
        const SaslMechanismInfo* getMechInfo() const { return _mechInfo; }

    private:
        static const int maxCallbacks = 4;
        sasl_conn_t* _saslConnection;
        sasl_callback_t _callbacks[maxCallbacks];
        const SaslMechanismInfo* _mechInfo;
    };
}  // namespace mongo
