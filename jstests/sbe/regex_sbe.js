/**
 * Test that SBE works correct for match expressions using a regex.
 */
(function() {
"use strict";

load("jstests/sbe/utils.js");  // For 'testCursorCommandSBE()'.

const coll = db.regex_sbe;
coll.drop();

assert.commandWorked(coll.insert([
    {_id: 1, a: "foobar"},
    {_id: 2, a: "baz"},
    {_id: 3, a: [1, 2, "foobar", 4]},
    {_id: 4, a: [1, 2, 3, 4]},
    {_id: 5, a: [{b: "baz"}, {b: "foobar"}]},
    {_id: 6, a: [{b: 1}, {b: 2}]},
    {_id: 7, a: [{b: [1, 2, "baz"]}, {b: [3, 4, "foo"]}]},
    {_id: 8, a: [{b: [1, 2, "baz"]}, {b: [3, 4]}]},
    {_id: 9, a: [{b: [1, 2, "baz"]}, {b: [[3, 4, "foo"]]}]},
    {_id: 10, a: [[{b: [1, 2, "baz"]}, {b: [3, 4, "foo"]}]]},
    {_id: 11, a: [{b: [1, 2, "baz"]}, {c: [3, 4, "foo"]}]},
]));

function testRegex(filter) {
    const expectSort = false;
    testCursorCommandSBE(db, {find: coll.getName(), filter: filter}, expectSort);
}

testRegex({a: /foo/});
testRegex({"a.b": /foo/});
testRegex({a: /foo/, "a.b": /foo/});
testRegex({a: /baz/});
testRegex({"a.b": /baz/});
}());
