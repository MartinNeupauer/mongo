// Test oplog queries that can be optimized with oplogReplay.
// @tags: [requires_replication, requires_capped]
//
// TODO: this test is a partial copy of jstests/noPassthroughWithMongod/query_oplogreplay.js and
// can be safely removed when SBE implementation is completed.

(function() {
"use strict";

load("jstests/libs/storage_engine_utils.js");

const t = db.getSiblingDB("local").oplog.sbe_oplog;

function dropOplogAndCreateNew(oplog, newCollectionSpec) {
    if (storageEngineIsWiredTigerOrInMemory()) {
        // We forbid dropping the oplog when using the WiredTiger or in-memory storage engines
        // and so we can't drop the oplog here. Because Evergreen reuses nodes for testing,
        // the oplog may already exist on the test node; in this case, trying to create the
        // oplog once again would fail.
        // To ensure we are working with a clean oplog (an oplog without entries), we resort
        // to truncating the oplog instead.
        if (!oplog.getDB().getCollectionNames().includes(oplog.getName())) {
            oplog.getDB().createCollection(oplog.getName(), newCollectionSpec);
        }
        oplog.runCommand('emptycapped');
        oplog.getDB().adminCommand({replSetResizeOplog: 1, size: 16 * 1024});
    } else {
        oplog.drop();
        assert.commandWorked(oplog.getDB().createCollection(oplog.getName(), newCollectionSpec));
    }
}

dropOplogAndCreateNew(t, {capped: true, size: 16 * 1024});

/**
 * Helper function for making timestamps with the property that if i < j, then makeTS(i) <
 * makeTS(j).
 */
function makeTS(i) {
    return Timestamp(1000, i);
}

for (let i = 1; i <= 100; i++) {
    assert.commandWorked(t.insert({_id: i, ts: makeTS(i)}));
}

// A $gt query on just the 'ts' field should return the next document after the timestamp.
var cursor = t.find({ts: {$gt: makeTS(20)}});
assert.eq(21, cursor.next()["_id"]);
assert.eq(22, cursor.next()["_id"]);

// A $gte query on the 'ts' field should include the timestamp.
cursor = t.find({ts: {$gte: makeTS(20)}});
assert.eq(20, cursor.next()["_id"]);
assert.eq(21, cursor.next()["_id"]);

// An $eq query on the 'ts' field should return the single record with the timestamp.
cursor = t.find({ts: {$eq: makeTS(20)}});
assert.eq(20, cursor.next()["_id"]);
assert(!cursor.hasNext());

// An AND with both a $gt and $lt query on the 'ts' field will correctly return results in
// the proper bounds.
cursor = t.find({$and: [{ts: {$lt: makeTS(5)}}, {ts: {$gt: makeTS(1)}}]});
assert.eq(2, cursor.next()["_id"]);
assert.eq(3, cursor.next()["_id"]);
assert.eq(4, cursor.next()["_id"]);
assert(!cursor.hasNext());

// An AND with multiple predicates on the 'ts' field correctly returns results on the
// tightest range.
cursor = t.find({
    $and: [
        {ts: {$gte: makeTS(2)}},
        {ts: {$gt: makeTS(3)}},
        {ts: {$lte: makeTS(7)}},
        {ts: {$lt: makeTS(7)}}
    ]
});
assert.eq(4, cursor.next()["_id"]);
assert.eq(5, cursor.next()["_id"]);
assert.eq(6, cursor.next()["_id"]);
assert(!cursor.hasNext());

// An AND with an $eq predicate in conjunction with other bounds correctly returns one
// result.
cursor = t.find({
    $and: [
        {ts: {$gte: makeTS(1)}},
        {ts: {$gt: makeTS(2)}},
        {ts: {$eq: makeTS(5)}},
        {ts: {$lte: makeTS(8)}},
        {ts: {$lt: makeTS(8)}}
    ]
});
assert.eq(5, cursor.next()["_id"]);
assert(!cursor.hasNext());

// Oplog replay optimization should work with projection.
let res = t.find({ts: {$lte: makeTS(4)}}).projection({'_id': 0});
while (res.hasNext()) {
    const next = res.next();
    assert(!next.hasOwnProperty('_id'));
    assert(next.hasOwnProperty('ts'));
}

res = t.find({ts: {$gte: makeTS(90)}}).projection({'_id': 0});
while (res.hasNext()) {
    const next = res.next();
    assert(!next.hasOwnProperty('_id'));
    assert(next.hasOwnProperty('ts'));
}

// Oplog replay optimization should work with limit.
res = t.find({$and: [{ts: {$gte: makeTS(4)}}, {ts: {$lte: makeTS(8)}}]}).limit(2).toArray();
assert.eq(2, res.length);

// A query over both 'ts' and '_id' should only pay attention to the 'ts' field for finding
// the oplog start (SERVER-13566).
cursor = t.find({ts: {$gte: makeTS(20)}, _id: 25});
assert.eq(25, cursor.next()["_id"]);
assert(!cursor.hasNext());
})();
