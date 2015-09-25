// SERVER-19095: Add $lookup aggregation stage.

// For assertErrorCode.
load("jstests/aggregation/extras/utils.js");

(function() {
    "use strict";

    // Used by testPipeline to sort result documents. All _ids must be primitives.
    function compareId(a, b) {
      if (a._id < b._id) {
        return -1;
      }
      if (a._id > b._id) {
        return 1;
      }
      return 0;
    }

    // Helper for testing that pipeline returns correct set of results.
    function testPipeline(pipeline, expectedResult, collection) {
        assert.eq(collection.aggregate(pipeline).toArray().sort(compareId),
            expectedResult.sort(compareId));
    }

    function runTest(coll, from) {
        var db = null; // Using the db variable is banned in this function.

        assert.writeOK(coll.insert({_id: 0, a: 1}));
        assert.writeOK(coll.insert({_id: 1, a: null}));
        assert.writeOK(coll.insert({_id: 2}));

        assert.writeOK(from.insert({_id: 0, b: 1}));
        assert.writeOK(from.insert({_id: 1, b: null}));
        assert.writeOK(from.insert({_id: 2}));

        //
        // Basic functionality.
        //

        // "from" document added to "as" field if a == b, where nonexistent fields are treated as
        // null.
        var expectedResults = [
            {_id: 0, a: 1, "same": [{_id: 0, b: 1}]},
            {_id: 1, a: null, "same": [{_id: 1, b: null}, {_id: 2}]},
            {_id: 2, "same": [{_id: 1, b: null}, {_id: 2}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }], expectedResults, coll);

        // If localField is nonexistent, it is treated as if it is null.
        expectedResults = [
            {_id: 0, a: 1, "same": [{_id: 1, b: null}, {_id: 2}]},
            {_id: 1, a: null, "same": [{_id: 1, b: null}, {_id: 2}]},
            {_id: 2, "same": [{_id: 1, b: null}, {_id: 2}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "nonexistent",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }], expectedResults, coll);

        // If foreignField is nonexistent, it is treated as if it is null.
        expectedResults = [
            {_id: 0, a: 1, "same": []},
            {_id: 1, a: null, "same": [{_id: 0, b: 1}, {_id: 1, b: null}, {_id: 2}]},
            {_id: 2, "same": [{_id: 0, b: 1}, {_id: 1, b: null}, {_id: 2}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "nonexistent",
                from: "from",
                as: "same"
            }
        }], expectedResults, coll);

        // If there are no matches or the from coll doesn't exist, the result is an empty array.
        expectedResults = [
            {_id: 0, a: 1, "same": []},
            {_id: 1, a: null, "same": []},
            {_id: 2, "same": []}
        ];
        testPipeline([{
            $lookup: {
                localField: "_id",
                foreignField: "nonexistent",
                from: "from",
                as: "same"
            }
        }], expectedResults, coll);
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "nonexistent",
                as: "same"
            }
        }], expectedResults, coll);

        // If field name specified by "as" already exists, it is overwritten.
        expectedResults = [
            {_id: 0, "a": [{_id: 0, b: 1}]},
            {_id: 1, "a": [{_id: 1, b: null}, {_id: 2}]},
            {_id: 2, "a": [{_id: 1, b: null}, {_id: 2}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "a"
            }
        }], expectedResults, coll);


        // Running multiple $lookups in the same pipeline is allowed.
        expectedResults = [
            {_id: 0, a: 1, "c": [{_id:0, b:1}], "d": [{_id:0, b:1}]},
            {_id: 1, a: null, "c": [{_id:1, b:null}, {_id:2}], "d": [{_id:1, b:null}, {_id:2}]},
            {_id: 2, "c": [{_id:1, b:null}, {_id:2}], "d": [{_id:1, b:null}, {_id:2}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "c"
            }
        }, {
            $project: {
                "a": 1,
                "c": 1
            }
        }, {
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "d"
            }
        }], expectedResults, coll);

        //
        // Coalescing with $unwind.
        //

        // A normal $unwind with on the "as" field.
        expectedResults = [
            {_id: 0, a: 1, same: {_id: 0, b: 1}},
            {_id: 1, a: null, same: {_id: 1, b: null}},
            {_id: 1, a: null, same: {_id: 2}},
            {_id: 2, same: {_id: 1, b: null}},
            {_id: 2, same: {_id: 2}}
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }, {
            $unwind: {path: "$same"}
        }], expectedResults, coll);

        // An $unwind on the "as" field, with includeArrayIndex.
        expectedResults = [
            {_id: 0, a: 1, same: {_id: 0, b: 1}, index: NumberLong(0)},
            {_id: 1, a: null, same: {_id: 1, b: null}, index: NumberLong(0)},
            {_id: 1, a: null, same: {_id: 2}, index: NumberLong(1)},
            {_id: 2, same: {_id: 1, b: null}, index: NumberLong(0)},
            {_id: 2, same: {_id: 2}, index: NumberLong(1)},
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }, {
            $unwind: {
                path: "$same",
                includeArrayIndex: "index"
            }
        }], expectedResults, coll);

        // Normal $unwind with no matching documents.
        expectedResults = [];
        testPipeline([{
            $lookup: {
                localField: "_id",
                foreignField: "nonexistent",
                from: "from",
                as: "same"
            }
        }, {
            $unwind: {path: "$same"}
        }], expectedResults, coll);

        // $unwind with preserveNullAndEmptyArray with no matching documents.
        expectedResults = [
            {_id: 0, a: 1, same: []},
            {_id: 1, a: null, same: []},
            {_id: 2, same: []},
        ];
        testPipeline([{
            $lookup: {
                localField: "_id",
                foreignField: "nonexistent",
                from: "from",
                as: "same"
            }
        }, {
            $unwind: {
                path: "$same",
                preserveNullAndEmptyArrays: true
            }
        }], expectedResults, coll);

        // $unwind with preserveNullAndEmptyArray, some with matching documents, some without.
        expectedResults = [
            {_id: 0, a: 1, same: []},
            {_id: 1, a: null, same: {_id: 0, b: 1}},
            {_id: 2, same: []},
        ];
        testPipeline([{
            $lookup: {
                localField: "_id",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }, {
            $unwind: {
                path: "$same",
                preserveNullAndEmptyArrays: true
            }
        }], expectedResults, coll);

        // $unwind with preserveNullAndEmptyArray and includeArrayIndex, some with matching
        // documents, some without.
        expectedResults = [
            {_id: 0, a: 1, same: [], index: null},
            {_id: 1, a: null, same: {_id: 0, b: 1}, index: NumberLong(0)},
            {_id: 2, same: [], index: null},
        ];
        testPipeline([{
            $lookup: {
                localField: "_id",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }, {
            $unwind: {
                path: "$same",
                preserveNullAndEmptyArrays: true,
                includeArrayIndex: "index"
            }
        }], expectedResults, coll);

        //
        // Dependencies.
        //

        // If $lookup didn't add "localField" to its dependencies, this test would fail as the
        // value of the "a" field would be lost and treated as null.
        expectedResults = [
            {_id: 0, "same": [{_id: 0, b: 1}]},
            {_id: 1, "same": [{_id: 1, b: null}, {_id: 2}]},
            {_id: 2, "same": [{_id: 1, b: null}, {_id: 2}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }, {
            $project: {
                "same": 1
            }
        }], expectedResults, coll);

        //
        // Dotted field paths.
        //

        assert.writeOK(coll.insert({_id: 3, a: {c : 1}}));

        assert.writeOK(from.insert({_id: 3, b: {c : 1}}));
        assert.writeOK(from.insert({_id: 4, b: {c : 2}}));

        expectedResults = [
            {_id: 0, a: 1, "same": [{_id: 0, b: 1}]},
            {_id: 1, a: null, "same": [{_id: 1, b: null}, {_id: 2}]},
            {_id: 2, "same": [{_id: 1, b: null}, {_id: 2}]},
            {_id: 3, a: {c: 1}, "same": [{_id: 3, b: {c: 1}}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "a",
                foreignField: "b",
                from: "from",
                as: "same"
            }
        }], expectedResults, coll);

        expectedResults = [
            {_id: 0, a: 1, "same": [{_id: 0, b: 1}, {_id: 1, b: null}, {_id: 2}]},
            {_id: 1, a: null, "same": [{_id: 0, b: 1}, {_id: 1, b: null}, {_id: 2}]},
            {_id: 2, "same": [{_id: 0, b: 1}, {_id: 1, b: null}, {_id: 2}]},
            {_id: 3, a: {c: 1}, "same": [{_id: 3, b: {c: 1}}]}
        ];
        testPipeline([{
            $lookup: {
                localField: "a.c",
                foreignField: "b.c",
                from: "from",
                as: "same"
            }
        }], expectedResults, coll);

        //
        // Error cases.
        //

        // All four fields must be specified.
        assertErrorCode(coll, [{$lookup: {foreignField:"b", from:"from", as:"same"}}], 4572);
        assertErrorCode(coll, [{$lookup: {localField:"a", from:"from", as:"same"}}], 4572);
        assertErrorCode(coll, [{$lookup: {localField:"a", foreignField:"b", as:"same"}}], 4572);
        assertErrorCode(coll, [{$lookup: {localField:"a", foreignField:"b", from:"from"}}], 4572);

        // All four field's values must be strings.
        assertErrorCode(coll, [{$lookup: {localField:1, foreignField:"b", from:"from", as:"as"}}]
            , 4570);
        assertErrorCode(coll, [{$lookup: {localField:"a", foreignField:1, from:"from", as:"as"}}]
            , 4570);
        assertErrorCode(coll, [{$lookup: {localField:"a", foreignField:"b", from:1, as:"as"}}]
            , 4570);
        assertErrorCode(coll, [{$lookup: {localField:"a", foreignField: "b", from:"from", as:1}}]
            , 4570);

        // $lookup's field must be an object.
        assertErrorCode(coll, [{$lookup: "string"}], 4569);
    }

    // Run tests on single node.
    db.lookUp.drop();
    db.from.drop();
    runTest(db.lookUp, db.from);

    // Run tests in a sharded environment.
    var sharded = new ShardingTest({shards: 2, verbose: 0, mongos: 1});
    assert(sharded.adminCommand({enableSharding : "test"}));
    sharded.getDB('test').lookUp.drop();
    sharded.getDB('test').from.drop();
    assert(sharded.adminCommand({shardCollection: "test.lookUp", key: {_id: 'hashed'}}));
    runTest(sharded.getDB('test').lookUp, sharded.getDB('test').from);

    // An error is thrown if the from collection is sharded.
    assert(sharded.adminCommand({ shardCollection:"test.from", key: {_id: 1}}));
    assertErrorCode(sharded.getDB('test').lookUp, [{
        $lookup: {
            localField: "a",
            foreignField: "b",
            from: "from",
            as: "same"
        }
    }], 28769);
    sharded.stop();
}());
