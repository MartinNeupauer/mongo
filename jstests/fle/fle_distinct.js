/**
 * Basic set of tests to verify the response from mongocryptd for the distinct command.
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
                    initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ==")
                }
            },
            ssnWithPointer: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: "/whoKnows",
                    initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ==")
                }
            }
        }
    };

    // Test that encrypted fields in the query are correctly replaced with an encryption
    // placeholder.
    let res = assert.commandWorked(testDB.runCommand(
        {distinct: "test", key: "a", query: {ssn: {$eq: 5}}, jsonSchema: sampleSchema}));
    assert.eq(res.result.distinct, "test", tojson(res));
    assert.eq(res.hasEncryptionPlaceholders, true, tojson(res));
    assert(res.result.query.ssn.$eq instanceof BinData, tojson(res));

    // Test that a missing distinct key correctly fails.
    assert.commandFailedWithCode(testDB.runCommand({distinct: "test", jsonSchema: sampleSchema}),
                                 40414);

    // Test that the command fails if the distinct key is an encrypted field with a JSON Pointer
    // keyId.
    assert.commandFailedWithCode(
        testDB.runCommand({distinct: "test", key: "ssnWithPointer", jsonSchema: sampleSchema}),
        51131);

    // Test that invalid generic command arguments are ignored. The rationale for this is that there
    // is no sensitive/encrypted data within these options.
    assert.commandWorked(testDB.runCommand(
        {distinct: "test", key: "ssn", readConcern: "invalid", jsonSchema: sampleSchema}));
    assert.commandWorked(
        testDB.runCommand({distinct: "test", key: "ssn", maxTimeMS: -1, jsonSchema: sampleSchema}));

    // Test that a distinct command with unknown fields correctly fails.
    assert.commandFailedWithCode(
        testDB.runCommand(
            {distinct: "test", key: "ssn", invalidFieldName: true, jsonSchema: sampleSchema}),
        40415);

    mongocryptd.stop();
})();
