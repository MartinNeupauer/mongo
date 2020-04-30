/**
 * Test that $text search queries can be executed correctly using the SBE engine.
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For arrayEq.

const coll = db.sbe_agg;
coll.drop();

function toggleSBE(isSBE) {
    assert.commandWorked(
        db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": isSBE}));
}

assert.commandWorked(coll.createIndex({a: "text"}));
assert.commandWorked(coll.insert([
    {_id: 1, a: "one"},
    {_id: 2, a: "one two"},
    {_id: 3, a: "one two three"},
    {_id: 4, a: "three five"},
    {_id: 5, a: "one give"},
    {_id: 6, a: "two three"}
]));

function testTextSearch(textQuery) {
    const filter = {$text: {$search: textQuery}};
    toggleSBE(true);
    const resultsWithSBE = coll.find(filter).toArray();
    toggleSBE(false);
    const resultsWithDefault = coll.find(filter).toArray();
    assert(arrayEq(resultsWithSBE, resultsWithDefault),
           `actual=${tojson(resultsWithSBE)}, expected=${tojson(resultsWithDefault)}, textQuery=${
               tojson(textQuery)}`);
}

testTextSearch("one");
testTextSearch("one two");
testTextSearch("one two three");
testTextSearch("\"one two\"");
testTextSearch("\"one two\" three");
testTextSearch("one -five");
}());
