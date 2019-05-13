/**
 * Verify the KMS support handles a buggy Key Store
 */

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mock_kms.js");
load('jstests/ssl/libs/ssl_helpers.js');

(function() {
    "use strict";

    const mock_kms = new MockKMSServer();
    mock_kms.start();

    const x509_options = {sslMode: "requireSSL", sslPEMKeyFile: SERVER_CERT, sslCAFile: CA_CERT};

    const conn = MongoRunner.runMongod(x509_options);
    const test = conn.getDB("test");
    const collection = test.coll;

    const awsKMS = {
        accessKeyId: "access",
        secretAccessKey: "secret",
        url: mock_kms.getURL(),
    };

    var localKMS = {
        key: BinData(
            0,
            "/i8ytmWQuCe1zt3bIuVa4taPGKhqasVp0/0yI4Iy0ixQPNmeDF1J5qPUbBYoueVUJHMqj350eRTwztAWXuBdSQ=="),
    };

    const clientSideFLEOptions = {
        kmsProviders: {
            aws: awsKMS,
            local: localKMS,
        },
        keyVaultCollection: collection,
    };

    function testFault(kmsType, func) {
        collection.drop();

        const shell = Mongo(conn.host, clientSideFLEOptions);
        const keyStore = shell.getKeyStore();

        assert.writeOK(
            keyStore.createKey(kmsType, "arn:aws:kms:us-east-1:fake:fake:fake", ['mongoKey']));
        const keyId = keyStore.getKeyByAltName("mongoKey").toArray()[0]._id;

        func(keyId, shell);
    }

    function testFaults(func) {
        const kmsTypes = ["aws", "local"];

        for (const kmsType of kmsTypes) {
            testFault(kmsType, func);
        }
    }

    // Negative - drop the key vault collection
    testFaults((keyId, shell) => {
        collection.drop();

        const str = "mongo";
        assert.throws(() => {
            const encStr = shell.encrypt(keyId, str);
        });
    });

    // Negative - delete the keys
    testFaults((keyId, shell) => {
        collection.deleteMany({});

        const str = "mongo";
        assert.throws(() => {
            const encStr = shell.encrypt(keyId, str);
        });
    });

    // Negative - corrupt the master key with an unkown provider
    testFaults((keyId, shell) => {
        collection.updateMany({}, {$set: {"masterKey.provider": "fake"}});

        const str = "mongo";
        assert.throws(() => {
            const encStr = shell.encrypt(keyId, str);
        });
    });

    MongoRunner.stopMongod(conn);
    mock_kms.stop();
}());