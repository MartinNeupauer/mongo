// Validate fle accepts commands with and without json schema for non-encrypted commands.
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");

    const basicJSONSchema = {properties: {foo: {type: "string"}}};
    const basicEncryptSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ=="),
                    keyId: [UUID("4edee966-03cc-4525-bfa8-de8acd6746fa")]
                }
            }
        }
    };

    let cmds = [
        {find: "foo", filter: {_id: 1}},
        {distinct: "foo", query: {_id: 1}, key: "_id"},
        {count: "foo", query: {_id: 1}},
        {findAndModify: "foo", query: {foo: 1}, update: {$inc: {score: 1.0}}},
        // old name
        {findandmodify: "foo", query: {foo: 1}, update: {$inc: {score: 1.0}}},
        {aggregate: "foo", pipeline: [{filter: {$eq: 1.0}}]},
        {insert: "foo", documents: [{foo: 1}]},
        {update: "foo", updates: [{q: {foo: 1}, u: {"$set": {a: 2}}}]},
        {delete: "foo", deletes: [{q: {foo: 1}, limit: 1}]},
    ];

    const supportedCommands = [
        "count",
        "delete",
        "distinct",
        "find",
        "findandmodify",
        "findAndModify",
        "insert",
        "updates",
    ];

    cmds.forEach(element => {
        // Make sure no json schema fails
        assert.commandFailed(testDB.runCommand(element));

        // NOTE: This mutates element so it now has jsonSchema
        Object.extend(element, {jsonSchema: basicJSONSchema});

        // Make sure json schema works
        const ret1 = assert.commandWorked(testDB.runCommand(element));
        assert.eq(ret1.hasEncryptionPlaceholders, false, ret1);
        assert.eq(ret1.schemaRequiresEncryption, false, ret1);

<<<<<<< HEAD
=======
        const explain_good = {
            explain: 1,
            explain: element,  // eslint-disable-line no-dupe-keys
        };

        // Make sure json schema works when explaining
        const ret2 = assert.commandWorked(testDB.runCommand(explain_good));
        assert.eq(ret2.hasEncryptionPlaceholders, false, ret2);
        assert.eq(ret2.schemaRequiresEncryption, false, ret2);

>>>>>>> SERVER-40005 Add translation for FindAndModify for FLE
        // Test that generic "passthrough" command arguments are correctly echoed back from
        // mongocryptd.
        const passthroughFields = {
            "writeConcern": {w: "majority", wtimeout: 5000},
            "$audit": "auditString",
            "$client": "clientString",
            "$configServerState": 2,
            "allowImplicitCollectionCreation": true,
            "$oplogQueryData": false,
            "$readPreference": {"mode": "primary"},
            "$replData": {data: "here"},
            "$clusterTime": "now",
            "maxTimeMS": 500,
            "readConcern": {level: "majority"},
            "databaseVersion": 2.2,
            "shardVersion": 4.6,
            "tracking_info": {"found": "it"},
            "txnNumber": 7,
            "autocommit": false,
            "coordinator": false,
            "startTransaction": "now",
            "stmtId": NumberInt(1),
            "lsid": 1,
        };

        // Switch to the schema containing encrypted fields.
        Object.assign(element, {jsonSchema: basicEncryptSchema});

        // Merge the passthrough fields with the current command object.
        Object.assign(element, passthroughFields);

        const passthroughResult = assert.commandWorked(testDB.runCommand(element));

        if (Object.keys(element).some(field => supportedCommands.includes(field))) {
            // Command is supported, verify that each of the passthrough fields is included in the
            // result.
            for (let field in passthroughFields) {
                assert.eq(
                    passthroughResult.result[field], passthroughFields[field], passthroughResult);

                // Verify that the 'schemaRequiresEncryption' bit is correctly set.
                assert.eq(passthroughResult.schemaRequiresEncryption, true, passthroughResult);
            }

            // The '$db' field is special as it's automatically added by the shell.
            assert.eq(passthroughResult.result.$db, "test", passthroughResult);
        } else {
            // Command is not supported yet, verify an empty 'result' in the response.
            assert.eq(passthroughResult.result, {}, passthroughResult);
        }
    });

    mongocryptd.stop();
})();
