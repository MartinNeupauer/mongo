/**
 * Test that $text search queries can be executed correctly using the SBE engine.
 */
(function() {
"use strict";

load("jstests/sbe/utils.js");  // For 'testCursorCommandSBE()'.

const coll = db.fts_sbe;
coll.drop();

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
    const expectSort = false;
    const filter = {$text: {$search: textQuery}};
    testCursorCommandSBE(db, {find: coll.getName(), filter: filter}, expectSort);
}

testTextSearch("one");
testTextSearch("one two");
testTextSearch("one two three");
testTextSearch("\"one two\"");
testTextSearch("\"one two\" three");
testTextSearch("one -five");
}());
