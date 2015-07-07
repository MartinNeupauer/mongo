/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <string>

#include "encryption_key_manager_noop.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_recovery_unit.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace {
MONGO_INITIALIZER_WITH_PREREQUISITES(CreateEncryptionKeyManager,
                                     ("SetWiredTigerCustomizationHooks", "SetGlobalEnvironment"))
(InitializerContext* context) {
    // Reset the WiredTigerCustomizationHooks pointer to be the EncryptionKeyManager
    auto keyManager = stdx::make_unique<EncryptionKeyManagerNoop>();
    WiredTigerCustomizationHooks::set(getGlobalServiceContext(), std::move(keyManager));

    return Status::OK();
}

class WiredTigerConnection {
public:
    WiredTigerConnection(StringData dbpath) : _conn(NULL) {
        std::stringstream ss;
        ss << "create,cache_size=100MB,";
        ss << "extensions=[local=(entry=mongo_addWiredTigerEncryptors)],";
        ss << "encryption=(name=aes,keyid=system),";
        std::string config = ss.str();
        int ret = wiredtiger_open(dbpath.toString().c_str(), NULL, config.c_str(), &_conn);

        ASSERT_OK(wtRCToStatus(ret));
        ASSERT(_conn);
    }
    ~WiredTigerConnection() {
        _conn->close(_conn, NULL);
    }
    WT_CONNECTION* getConnection() const {
        return _conn;
    }

private:
    WT_CONNECTION* _conn;
};

class WiredTigerUtilHarnessHelper {
public:
    WiredTigerUtilHarnessHelper(const std::string& dbPath)
        : _connection(dbPath), _sessionCache(_connection.getConnection()) {}

    WiredTigerSessionCache* getSessionCache() {
        return &_sessionCache;
    }

    OperationContext* newOperationContext() {
        return new OperationContextNoop(new WiredTigerRecoveryUnit(getSessionCache()));
    }

private:
    WiredTigerConnection _connection;
    WiredTigerSessionCache _sessionCache;
};


void writeData(std::string dbPath) {
    WiredTigerUtilHarnessHelper harnessHelper(dbPath);
    WiredTigerRecoveryUnit* ru = new WiredTigerRecoveryUnit(harnessHelper.getSessionCache());
    OperationContextNoop txn(ru);
    WiredTigerSession* mongoSession = ru->getSession(nullptr);

    WriteUnitOfWork uow(&txn);
    WT_SESSION* session = mongoSession->getSession();

    /*
    * Create and open some encrypted and not encrypted tables.
    */
    ASSERT_OK(wtRCToStatus(session->create(session,
                                           "table:crypto1",
                                           "encryption=(name=aes,keyid=abc),"
                                           "columns=(key0,value0),"
                                           "key_format=S,value_format=S")));
    ASSERT_OK(wtRCToStatus(session->create(session,
                                           "table:crypto2",
                                           "encryption=(name=aes,keyid=efg),"
                                           "columns=(key0,value0),"
                                           "key_format=S,value_format=S")));
    WT_CURSOR* c1, *c2;
    ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto1", NULL, NULL, &c1)));
    ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto2", NULL, NULL, &c2)));

    /*
    * Insert a set of keys and values.  Insert the same data into
    * all tables so that we can verify they're all the same after
    * we decrypt on read.
    */
    char keybuf[16], valbuf[16];
    for (int i = 0; i < 10; i++) {
        snprintf(keybuf, sizeof(keybuf), "key%d", i);
        c1->set_key(c1, keybuf);
        c2->set_key(c2, keybuf);

        snprintf(valbuf, sizeof(valbuf), "value%d", i);
        c1->set_value(c1, valbuf);
        c2->set_value(c2, valbuf);

        c1->insert(c1);
        c2->insert(c2);
    }

    uow.commit();
}

void readData(std::string dbPath) {
    WiredTigerUtilHarnessHelper harnessHelper(dbPath);
    WiredTigerRecoveryUnit recoveryUnit(harnessHelper.getSessionCache());
    WiredTigerSession* mongoSession = recoveryUnit.getSession(NULL);
    WT_SESSION* session = mongoSession->getSession();

    WT_CURSOR* c1, *c2;
    ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto1", NULL, NULL, &c1)));
    ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto2", NULL, NULL, &c2)));

    char* key1, *val1, *key2, *val2;
    int ret;
    while (c1->next(c1) == 0) {
        ret = c2->next(c2);
        ret = c1->get_key(c1, &key1);
        ret = c1->get_value(c1, &val1);
        ret = c2->get_key(c2, &key2);
        ret = c2->get_value(c2, &val2);

        ASSERT(strcmp(key1, key2) == 0) << "Key1 " << key1 << " and Key2 " << key2
                                        << " do not match";
        ASSERT(strcmp(val1, val2) == 0) << "Val1 " << val1 << " and Val2 " << val2
                                        << " do not match";
    }
}

TEST(WiredTigerEncryptionTest, ReadWriteData) {
    unittest::TempDir dbPath("wt_test");
    writeData(dbPath.path());
    readData(dbPath.path());
}

}  // namespace
}  // namespace mongo
