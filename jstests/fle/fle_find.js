/**
 * Basic set of tests to verify the response from mongocryptd for the find command.
 */
(function() {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");

    const sampleSchema = {
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [UUID()],
                    bsonType: "long"
                }
            },
            user: {
                type: "object",
                properties: {
                    account: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [UUID()],
                            bsonType: "string"
                        }
                    }
                }
            }
        }
    };

    function assertEncryptedFieldInResponse({filter, path = "", requiresEncryption}) {
        const res = assert.commandWorked(testDB.runCommand(
            {find: "test", filter: filter, jsonSchema: sampleSchema, isRemoteSchema: false}));

        assert(res.result.find == "test", tojson(res));
        assert(res.hasEncryptionPlaceholders == requiresEncryption, tojson(res));
        assert(path === "" || res.result.filter[path]["$eq"] instanceof BinData, tojson(res));
    }

    // Basic top-level field in equality correctly marked for encryption.
    assertEncryptedFieldInResponse(
        {filter: {ssn: NumberLong(5)}, path: "ssn", requiresEncryption: true});

    // Nested field in equality correctly marked for encryption.
    assertEncryptedFieldInResponse(
        {filter: {"user.account": "secret"}, path: "user.account", requiresEncryption: true});

    // Elements within $in array correctly marked for encryption.
    let res = assert.commandWorked(testDB.runCommand({
        find: "test",
        filter: {ssn: {"$in": [NumberLong(1234)]}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));
    assert(res.hasEncryptionPlaceholders, tojson(res));
    assert(res.result.filter["ssn"]["$in"][0] instanceof BinData, tojson(res));

    // Elements within object in $in array correctly marked for encryption.
    res = assert.commandWorked(testDB.runCommand({
        find: "test",
        filter: {user: {"$in": [{account: "1234"}]}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));
    assert(res.hasEncryptionPlaceholders, tojson(res));
    assert(res.result.filter["user"]["$in"][0]["account"] instanceof BinData, tojson(res));

    // Multiple elements inside $in array correctly marked for encryption.
    res = assert.commandWorked(testDB.runCommand({
        find: "test",
        filter: {ssn: {"$in": [NumberLong(1), NumberLong(2), NumberLong(3)]}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));
    assert(res.hasEncryptionPlaceholders, tojson(res));
    assert(res.result.filter["ssn"]["$in"][0] instanceof BinData, tojson(res));
    assert(res.result.filter["ssn"]["$in"][1] instanceof BinData, tojson(res));
    assert(res.result.filter["ssn"]["$in"][2] instanceof BinData, tojson(res));

    // Mixture of encrypted and non-encrypt elements inside $in array.
    res = assert.commandWorked(testDB.runCommand({
        find: "test",
        filter: {user: {"$in": ["notEncrypted", {also: "notEncrypted"}, {account: "encrypted"}]}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));
    assert(res.hasEncryptionPlaceholders, tojson(res));
    assert(res.result.filter["user"]["$in"][0] === "notEncrypted", tojson(res));
    assert(res.result.filter["user"]["$in"][1]["also"] === "notEncrypted", tojson(res));
    assert(res.result.filter["user"]["$in"][2]["account"] instanceof BinData, tojson(res));

    // Responses to queries without any encrypted fields should not set the
    // 'hasEncryptionPlaceholders' bit.
    assertEncryptedFieldInResponse({filter: {}, requiresEncryption: false});
    assertEncryptedFieldInResponse({filter: {"user.notSecure": 5}, requiresEncryption: false});
    assertEncryptedFieldInResponse(
        {filter: {user: {"$in": [{notSecure: 1}]}}, requiresEncryption: false});

    // Invalid operators should fail with an appropriate error code.
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {ssn: {$gt: 5}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 51118);
    assert.commandFailedWithCode(
        testDB.runCommand(
            {find: "test", filter: {ssn: /\d/}, jsonSchema: sampleSchema, isRemoteSchema: false}),
        51092);
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {ssn: {$in: [/\d/]}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 51015);

    // Invalid operators with encrypted fields in RHS object should fail.
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {user: {$gt: {account: "5"}}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 51119);

    // Comparison to a null value correctly fails.
    assert.commandFailedWithCode(
        testDB.runCommand(
            {find: "test", filter: {ssn: null}, jsonSchema: sampleSchema, isRemoteSchema: false}),
        51095);
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {ssn: {$in: [null]}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 51120);

    // Queries on paths which contain an encrypted prefixed field should fail.
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {'ssn.illegal': 5},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 51102);

    // Queries on paths which aren't described in the schema AND don't contain an encrypted prefix
    // should not fail.
    assert.doesNotThrow(() => testDB.runCommand({
        find: "test",
        filter: {'user.nonexistent': 5},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));

    // Invalid expressions correctly fail to parse.
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {$cantDoThis: 5},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 ErrorCodes.BadValue);

    // Unknown fields correctly result in an error.
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {},
        jsonSchema: sampleSchema,
        isRemoteSchema: false,
        whatIsThis: 1
    }),
                                 ErrorCodes.FailedToParse);

    // Invalid type for command parameters correctly result in an error.
    assert.commandFailedWithCode(
        testDB.runCommand({find: 5, filter: {}, jsonSchema: sampleSchema, isRemoteSchema: false}),
        ErrorCodes.InvalidNamespace);
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: "not an object",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 ErrorCodes.FailedToParse);
    assert.commandFailedWithCode(
        testDB.runCommand(
            {find: "test", filter: {}, jsonSchema: "same here", isRemoteSchema: false}),
        51090);

    // Verify that a schema with 'patternProperties' is supported by mongocryptd for the find
    // command.
    let cmdRes = assert.commandWorked(testDB.runCommand({
        find: "test",
        filter: {userSsn: "123-45-6789"},
        jsonSchema: {
            type: "object",
            patternProperties: {
                "[Ss]sn": {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [UUID()],
                        bsonType: "string"
                    }
                }
            }
        },
        isRemoteSchema: false
    }));
    assert(cmdRes.result.filter.userSsn.$eq instanceof BinData, tojson(cmdRes));

    // Verify that a find with a field which is encrypted with a JSONPointer keyId fails.
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {userSsn: "123-45-6789"},
        jsonSchema: {
            type: "object",
            properties: {
                userSsn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: "/key",
                        bsonType: "string"
                    }
                }
            }
        },
        isRemoteSchema: false
    }),
                                 51093);

    // Verify that a find with a randomized algorithm fails.
    assert.commandFailedWithCode(testDB.runCommand({
        find: "test",
        filter: {userSsn: "123-45-6789"},
        jsonSchema: {
            type: "object",
            properties: {
                userSsn: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [UUID()],
                    }
                }
            }
        },
        isRemoteSchema: false
    }),
                                 51158);

    mongocryptd.stop();
})();
