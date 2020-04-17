/**
 * Test that aggregate commands return the correct results when executed using SBE.
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For arrayEq and orderedArrayEq.

const coll = db.sbe_agg;
coll.drop();

assert.commandWorked(coll.insert([
    {_id: 1, a: 1, b: "x", c: 10},
    {_id: 2, a: 2, b: "y", c: 11},
    {_id: 3, a: 3, b: "z", c: 12},
    {_id: 5, x: {y: 1}},
    {_id: 6, x: {y: 2}},
    {_id: 7, x: {y: [1, 2, 3]}, v: {w: [4, 5, 6]}},
    {_id: 8, x: {y: 4}, v: {w: 4}},
    {_id: 9, x: [{y: 1}], v: [{w: 1}]},
    {_id: 10, x: [{y: 1}, {y: 2}], v: [{w: 5}, {w: 6}]},
    {_id: 11, x: [{y: 1}, {y: [1, 2, 3]}], v: [{w: 4}, {w: [4, 5, 6]}]},
    {_id: 12, z: 1},
    {_id: 13, z: 2},
    {_id: 14, z: [1, 2, 3]},
    {_id: 15, z: 3},
    {_id: 16, z: 4},
    {_id: 17, a: 10, x: 1},
    {_id: 18, a: 10, x: 10},
    {_id: 19, x: {y: [{z: 1}, {z: 2}]}},
    {_id: 20, x: [[{y: 1}, {y: 2}], {y: 3}, {y: 4}, [[[{y: 5}]]], {y: 6}]},
    {_id: 21, i: {j: 5}, k: {l: 10}},
    {_id: 22, x: [[{y: 1}, {y: 2}], {y: 3}, {y: 4}, [[[{y: 5}]]], {y: 6}]},
    {_id: 23, x: [[{y: {z: 1}}, {y: 2}], {y: 3}, {y: {z: 2}}, [[[{y: 5}, {y: {z: 3}}]]], {y: 6}]},
    {_id: 24, tf: [true, false], ff: [false, false], t: true, f: false, n: null, a: 1, b: 0},
    {_id: 25, i1: NumberInt(1), i2: NumberInt(-1), i3: NumberInt(-2147483648)},
    {_id: 26, l1: NumberLong("12345678900"), l2: NumberLong("-12345678900")},
    {_id: 27, s: "string", l: NumberLong("-9223372036854775808"), n: null},
    {_id: 28, d1: 4.6, d2: -4.6, dec1: NumberDecimal("4.6"), dec2: NumberDecimal("-4.6")}
]));

function toggleSBE(isSBE) {
    assert.commandWorked(
        db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": isSBE}));
}

// Runs 'pipeline' with SBE enabled and disabled, and checks that the result sets are the same. If
// 'expectSort' is true, then we verify that the two result sets have the same order. Otherwise, we
// check that the results are the same but permit different orders.
function testPipeline({pipeline, expectSort}) {
    toggleSBE(true);
    const resultsWithSBE = coll.aggregate(pipeline).toArray();
    toggleSBE(false);
    const resultsWithDefault = coll.aggregate(pipeline).toArray();

    if (expectSort) {
        assert(orderedArrayEq(resultsWithSBE, resultsWithDefault),
               `actual=${tojson(resultsWithSBE)}, expected=${tojson(resultsWithDefault)}, query=${
                   tojson(pipeline)}`);
    } else {
        assert(arrayEq(resultsWithSBE, resultsWithDefault),
               `actual=${tojson(resultsWithSBE)}, expected=${tojson(resultsWithDefault)}, query=${
                   tojson(pipeline)}`);
    }
}

testPipeline({pipeline: [{$match: {x: {$gt: 0}}}], expectSort: false});
testPipeline({pipeline: [{$match: {x: {$gt: 0}}}, {$sort: {x: -1}}], expectSort: true});
testPipeline({pipeline: [{$match: {x: {$gt: 0}}}, {$addFields: {foo: "bar"}}], expectSort: false});
testPipeline({
    pipeline: [{$match: {x: {$gt: 0}}}, {$sort: {x: -1}}, {$addFields: {foo: "bar"}}],
    expectSort: true
});
testPipeline({
    pipeline: [{$match: {_id: {$gt: 0}}}, {$group: {_id: null, allZs: {$push: "$z"}}}],
    expectSort: false
});
}());
