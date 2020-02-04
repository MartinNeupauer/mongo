// A sanity test which verifies various queries currently supported by the SBE engine for
// correctness.
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For arrayEq and orderedArrayEq.

const coll = db.sbe_sanity;
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
    {_id: 23, x: [[{y: {z: 1}}, {y: 2}], {y: 3}, {y: {z: 2}}, [[[{y: 5}, {y: {z: 3}}]]], {y: 6}]}
]));
let results;
let mongoResults;

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

    const queryObj = {query: query, proj: proj, sort: sort, limit: limit, skip: skip, hint: hint};
    results.push({query: queryObj, result: run(queryObj)});
}

[false, true].forEach((isSBE) => {
    assert.commandWorked(
        db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": isSBE}));
    results = [];

    [{hint: null, indexes: []}, {hint: {a: 1}, indexes: [{a: 1}]}, {
        hint: {z: 1},
        indexes: [{z: 1}]
    }].forEach((entry) => {
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
        runQuery({query: {'x.y': 5}, hint: hint});

        // Simple projections.
        // TODO: covered projections are not supported yet.
        if (!hint) {
            // Simple projection.
            runQuery({proj: {_id: 1}, hint: hint});
            runQuery({proj: {a: 1}, hint: hint});
            runQuery({proj: {z: 1}, hint: hint});
            runQuery({proj: {b: 1, a: 1}, hint: hint});
            runQuery({proj: {a: 1, _id: 0}, hint: hint});
            runQuery({query: {a: {$gt: 1}}, proj: {a: 1}, hint: hint});
            runQuery({proj: {a: 1, nonexistent: 1}, hint: hint});

            runQuery({proj: {_id: 0}, hint: hint});
            runQuery({proj: {a: 0}, hint: hint});
            runQuery({proj: {z: 0}, hint: hint});
            runQuery({proj: {b: 0, a: 0}, hint: hint});
            runQuery({proj: {a: 0, _id: 1}, hint: hint});
            runQuery({query: {a: {$gt: 1}}, proj: {a: 0}, hint: hint});
            runQuery({proj: {a: 0, nonexistent: 0}, hint: hint});

            // Dotted path.
            runQuery({proj: {_id: 1, 'x.y': 1}, hint: hint});
            runQuery({proj: {_id: 0, 'x.y': 1}, hint: hint});
            runQuery({proj: {'x.y': 1}, hint: hint});
            runQuery({proj: {'x.y.z': 1}, hint: hint});
            runQuery({proj: {'z.a': 1}, hint: hint});
            runQuery({proj: {'x.y': 1, 'v.w': 1}, hint: hint});
            runQuery({query: {'x.y': {$gt: 1}}, proj: {'v.w': 1}, hint: hint});
            runQuery({proj: {'x.y.nonexistent': 1}, hint: hint});

            runQuery({proj: {_id: 1, 'x.y': 0}, hint: hint});
            runQuery({proj: {_id: 0, 'x.y': 0}, hint: hint});
            runQuery({proj: {'x.y': 0}, hint: hint});
            runQuery({proj: {'x.y.z': 0}, hint: hint});
            runQuery({proj: {'z.a': 0}, hint: hint});
            runQuery({proj: {'x.y': 0, 'v.w': 0}, hint: hint});
            runQuery({query: {'x.y': {$gt: 1}}, proj: {'v.w': 0}, hint: hint});
            runQuery({proj: {'x.y.nonexistent': 0}, hint: hint});

            // Expressions.
            runQuery({proj: {_id: 1, foo: '$a'}, hint: hint});
            runQuery({proj: {_id: 0, foo: '$a'}, hint: hint});
            runQuery({proj: {foo: '$a'}, hint: hint});
            runQuery({proj: {foo: '$a', bar: '$b'}, hint: hint});
            runQuery({query: {c: {$gt: 0}}, proj: {foo: {$add: ['$a', '$c']}}, hint: hint});
            runQuery({proj: {a: 1, foo: '$b'}, hint: hint});
            runQuery({proj: {_id: 1, foo: '$x.y'}, hint: hint});
            runQuery({proj: {_id: 0, foo: '$x.y'}, hint: hint});
            runQuery({proj: {foo: '$x.y'}, hint: hint});
            runQuery({proj: {foo: '$x.y', bar: '$v.w'}, hint: hint});
            runQuery({query: {'i.j': {$gt: 0}}, proj: {foo: {$add: ['$i.j', '$k.l']}}, hint: hint});
            runQuery({proj: {'x.y': 1, foo: '$v.w'}, hint: hint});
        }
    });

    if (!isSBE) {
        mongoResults = results;
    } else {
        assert.eq(mongoResults.length, results.length);
        for (let i = 0; i < results.length; i++) {
            const query = results[i].query;
            const actual = results[i].result;
            const expected = mongoResults[i].result;
            if (!query.sort) {
                assert(arrayEq(actual, expected),
                       `actual=${tojson(actual)}, expected=${tojson(expected)}, query=${
                           tojson(query)}`);
            } else {
                assert(orderedArrayEq(actual, expected),
                       `actual=${tojson(actual)}, expected=${tojson(expected)}, query=${
                           tojson(query)}`);
            }
        }
    }
});
})();
