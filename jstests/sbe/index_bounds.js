/**
 * Test that index scans with various bounds return the correct results when executed using SBE.
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For arrayEq and orderedArrayEq.
load("jstests/libs/analyze_plan.js");         // For isIxscan.

const coll = db.index_bounds;
coll.drop();

const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < 30; i++) {
    for (let j = 0; j < 30; j++) {
        bulk.insert({a: i, b: j, c: i + j, d: i - j});
    }
}
assert.commandWorked(bulk.execute());

function toggleSBE(isSBE) {
    assert.commandWorked(
        db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": isSBE}));
}

// Runs 'query' with SBE enabled and disabled, and checks that the result sets are the same. Before
// executing the 'query' drops all indexes in the collection and created a new index using the
// 'indexSpec'.
function testQuery({query, indexSpec}) {
    assert.commandWorked(coll.dropIndexes());
    assert.commandWorked(coll.createIndex(indexSpec));

    toggleSBE(true);
    const resultsWithSBE = coll.find(query).toArray();
    assert.gt(resultsWithSBE.length, 0);

    toggleSBE(false);
    const resultsWithDefault = coll.find(query).toArray();
    assert.gt(resultsWithDefault.length, 0);

    // Make sure we had an IXSCAN plan indeed.
    const explain = assert.commandWorked(coll.find(query).explain());
    assert(isIxscan(db, explain.queryPlanner.winningPlan), explain);

    assert(arrayEq(resultsWithSBE, resultsWithDefault),
           `actual=${tojson(resultsWithSBE)}, expected=${tojson(resultsWithDefault)}, query=${
               tojson(query)}`);
}

testQuery({query: {a: 10, b: {$gt: 7}}, indexSpec: {a: 1, b: 1, c: 1}});
testQuery({query: {a: 5, c: {$lt: 25}}, indexSpec: {a: 1, b: 1, c: 1}});
testQuery({query: {a: {$gt: 20}, b: 6}, indexSpec: {a: 1, b: 1}});
testQuery({query: {a: 10, b: {$in: [10, 20, 25]}}, indexSpec: {a: 1, b: 1}});
testQuery({query: {$or: [{a: 10}, {a: 20}]}, indexSpec: {a: 1}});
testQuery({query: {a: {$gt: 4, $lt: 10}, c: {$gt: 20, $lt: 100}}, indexSpec: {a: 1, b: 1, c: 1}});
testQuery({query: {a: 20, b: {$gt: 3, $lte: 15}}, indexSpec: {a: 1, b: 1}});
testQuery({
    query: {a: {$gt: 12}, b: {$in: [3, 8, 1, 19]}, d: {$gt: -20, $lt: 20}},
    indexSpec: {a: 1, b: 1, c: 1, d: 1}
});

// Same queries as above but with the different index keys ordering.
testQuery({query: {a: 10, b: {$gt: 7}}, indexSpec: {a: 1, b: -1, c: 1}});
testQuery({query: {a: 5, c: {$lt: 25}}, indexSpec: {a: -1, b: -1, c: 1}});
testQuery({query: {a: {$gt: 20}, b: 6}, indexSpec: {a: -1, b: 1}});
testQuery({query: {a: 10, b: {$in: [10, 20, 25]}}, indexSpec: {a: -1, b: -1}});
testQuery({query: {$or: [{a: 10}, {a: 20}]}, indexSpec: {a: 1}});
testQuery({query: {a: {$gt: 4, $lt: 10}, c: {$gt: 20, $lt: 100}}, indexSpec: {a: 1, b: 1, c: -1}});
testQuery({query: {a: 20, b: {$gt: 3, $lte: 15}}, indexSpec: {a: -1, b: 1}});
testQuery({
    query: {a: {$gt: 12}, b: {$in: [3, 8, 1, 19]}, d: {$gt: -20, $lt: 20}},
    indexSpec: {a: -1, b: 1, c: 1, d: -1}
});
}());
