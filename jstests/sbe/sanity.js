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
    {_id: 17, a: 10, x: 1},
    {_id: 18, a: 10, x: 10},
]));

function runQuery(
    {query = {}, proj = {}, sort = null, hint = null, limit = null, skip = null} = {}) {
    const run = function() {
        let it = coll.find(query, proj);
        if (hint) {
            it = it.hint(hint);
        }
        if (sort) {
            it = it.sort(sort);
        }
        if (limit) {
            it = it.limit(limit);
        }
        if (skip) {
            it = it.skip(skip);
        }
        return it.toArray();
    };

    assert.commandWorked(
        db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": true}));
    const actual =
        run({query: query, proj: proj, sort: sort, limit: limit, skip: skip, hint: hint});

    assert.commandWorked(
        db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": false}));
    const expected =
        run({query: query, proj: proj, sort: sort, limit: limit, skip: skip, hint: hint});

    if (!sort) {
        assert(arrayEq(actual, expected), `actual=${tojson(actual)}, expected=${tojson(expected)}`);
    } else {
        assert(orderedArrayEq(actual, expected),
               `actual=${tojson(actual)}, expected=${tojson(expected)}`);
    }
}

[{hint: null, indexes: []}, {hint: {a: 1}, indexes: [{a: 1}]}, {hint: {z: 1}, indexes: [{z: 1}]}]
    .forEach((entry) => {
        assert.commandWorked(coll.dropIndexes());
        const hint = entry.hint;
        entry.indexes.forEach((index) => {
            assert.commandWorked(coll.createIndex(index));
        });

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
        runQuery({query: {a: 10, x: 1}, hint: hint});

        // Sort queries.
        // TODO: These queries do not produce stable deterministic results. Notably,
        // document not containing {a:...} form a tie set and there is not defined
        // orders for ties. The mongo and SBE implementations diverge at this point.
        if (!hint || hint == {a: 1}) {
            runQuery({sort: {a: 1}, hint: hint});
            runQuery({sort: {b: 1, a: 1}, hint: hint});
        }
        // These queries produce stable deterministic results (i.e. no ties).
        runQuery({query: {a: {$lt: 3}}, sort: {a: 1}, hint: hint});
        runQuery({query: {a: {$lte: 1}}, sort: {b: 1, a: 1}, hint: hint});
        runQuery({query: {a: {$lt: 3}}, sort: {a: 1}, limit: 1, hint: hint});
        runQuery({query: {a: {$lte: 3}}, sort: {b: 1, a: 1}, limit: 1, hint: hint});

        // Limit and skip queries.
        // TODO: Same comment applies as previous TODO.
        if (!hint) {
            runQuery({limit: 1, hint: hint});
            runQuery({a: {$gt: 1}, limit: 1, hint: hint});
            runQuery({a: 1, limit: 2, hint: hint});

            runQuery({skip: 1, hint: hint});
            runQuery({a: {$gt: 1}, skip: 1, hint: hint});
            runQuery({a: 1, skip: 2, hint: hint});

            runQuery({skip: 1, limit: 2, hint: hint});
            runQuery({a: {$gt: 1}, skip: 2, limit: 1, hint: hint});
            runQuery({a: 1, skip: 1, limit: 1, hint: hint});
        }

        // Path traversal in match expressions.
        runQuery({query: {'z': 2}, hint: hint});
        runQuery({query: {'x.y': 2}, hint: hint});

        // Simple projections.
        // TODO: covered projections are not supported yet.
        if (!hint) {
            runQuery({proj: {_id: 1}, hint: hint});
            runQuery({proj: {a: 1}, hint: hint});
            runQuery({proj: {z: 1}, hint: hint});
            runQuery({proj: {b: 1, a: 1}, hint: hint});
            runQuery({proj: {a: 1, _id: 0}, hint: hint});
            runQuery({query: {a: {$gt: 1}}, proj: {a: 1}, hint: hint});
            runQuery({proj: {a: 1, nonexistent: 1}, hint: hint});
        }
    });
})();
