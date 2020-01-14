// A sanity test which verifies various queries currently supported by the SBE engine for
// correctness.
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For arrayEq and orderedArrayEq.

const coll = db.sbe_sanity;
coll.drop();
assert.commandWorked(coll.insert([
    {_id: 1, a: 1, b: "x"},
    {_id: 2, a: 2, b: "y"},
    {_id: 3, a: 3, b: "z"},
    {_id: 5, x: {y: 1}},
    {_id: 6, x: {y: 2}},
    {_id: 7, x: {y: [1, 2, 3]}},
    {_id: 8, x: {y: 4}},
    {_id: 9, x: [{y: 1}]},
    {_id: 10, x: [{y: 1}, {y: 2}]},
    {_id: 11, x: [{y: 1}, {y: [1, 2, 3]}]},
    {_id: 12, z: 1},
    {_id: 13, z: 2},
    {_id: 14, z: [1, 2, 3]},
    {_id: 15, z: 3},
    {_id: 16, z: 4},
]));

function runDbQuery({query, proj, sort, hint, limit, skip}) {
    let dbQuery = coll.find(query, proj);
    if (hint) {
        dbQuery = dbQuery.hint(hint);
    }
    if (sort) {
        dbQuery = dbQuery.sort(sort);
    }
    if (limit) {
        dbQuery = dbQuery.limit(limit);
    }
    return dbQuery.toArray();
}

function runQuery(
    {query = {}, proj = {}, sort = null, hint = null, limit = null, skip = null} = {}) {
    db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": true});
    const actual =
        runDbQuery({query: query, proj: proj, sort: sort, limit: limit, skip: skip, hint: hint});

    db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": false});
    const expected =
        runDbQuery({query: query, proj: proj, sort: sort, limit: limit, skip: skip, hint: hint});

    if (!sort) {
        assert(arrayEq(actual, expected), `actual=${tojson(actual)}, expected=${tojson(expected)}`);
    } else {
        assert(orderedArrayEq(actual, expected),
               `actual=${tojson(actual)}, expected=${tojson(expected)}`);
    }
}

[{hint: null, indexes: []}, {hint: {a: 1}, indexes: [{a: 1}]}].forEach((entry) => {
    coll.dropIndexes();
    const hint = entry.hint;
    entry.indexes.forEach((index) => {coll.createIndex(index);});

    // Id hack.
    runQuery({query: {_id: 2}});

    // Empty filter.
    runQuery({hint: hint});

    // Point query.
    runQuery({query: {a: 2}, hint: hint});

    // Single interval queries.
    runQuery({query: {a: {$gt: 1}}, hint: hint});
    runQuery({query: {a: {$gte: 1}}, hint: hint});
    runQuery({query: {a: {$lt: 1}}, hint: hint});
    runQuery({query: {a: {$lte: 1}}, hint: hint});
    runQuery({query: {a: {$gt: 1, $lt: 3}}, hint: hint});
    runQuery({query: {a: {$gt: 1, $lte: 3}}, hint: hint});
    runQuery({query: {a: {$gte: 1, $lt: 3}}, hint: hint});
    runQuery({query: {a: {$gte: 1, $lte: 3}}, hint: hint});

    // Sort queries.
    runQuery({sort: {a: 1}, hint: hint});
    runQuery({sort: {b: 1, a: 1}, hint: hint});
    runQuery({query: {a: {$lt: 3}}, sort: {a: 1}, hint: hint});
    runQuery({query: {a: {$lte: 1}}, sort: {b: 1, a: 1}, hint: hint});

    // Limit queries.
    runQuery({limit: 1, hint: hint});
    runQuery({a: {$gt: 1}, limit: 1, hint: hint});
    runQuery({a: 1, limit: 2, hint: hint});

    // Path traversal in match expressions.
    runQuery({query: {'z': 2}});
    runQuery({query: {'x.y': 2}});
});
})();
