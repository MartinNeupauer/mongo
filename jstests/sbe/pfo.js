(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For arrayEq and orderedArrayEq.

const coll = db.sbe_pfo;

function runPFO(q, data, expected) {
    coll.drop();
    assert.commandWorked(coll.insert(data));

    q = "$pfo $$RESULT r [] " + q + " scan r [] sbe_pfo true";
    print("Running " + q);
    let actual = db.sbe(q).toArray();
    expected = [expected];

    assert(arrayEq(actual, expected),
           `actual=${tojson(actual)}, expected=${tojson(expected)}, query=${q}`);
}

runPFO("{_id:0}", {a: 10}, {a: 10});
// pick 'a' drop everything else
runPFO("{_id:0, a:1,~}", {a: 10, b: 10, c: 10}, {a: 10});
// drop 'a' keep everything else
runPFO("{_id:0, a:0}", {a: 10, b: 10, c: 10}, {b: 10, c: 10});
// set 'a' keep everything else
runPFO("{_id:0, a:='X'}", {a: 10, b: 10, c: 10}, {a: 'X', b: 10, c: 10});
// set 'a' and drop
runPFO("{_id:0, a:='X', ~}", {a: 10, b: 10, c: 10}, {a: 'X'});

runPFO("{_id:0, b:='X', ~}", {a: 10, b: {}, c: 10}, {b: 'X'});
runPFO("{_id:0, b:='X', ~}", {a: 10, b: [], c: 10}, {b: 'X'});

// drop non-exist - does not affect anything
runPFO("{_id:0, q:0}", {a: 10, b: 10, c: 10}, {a: 10, b: 10, c: 10});
runPFO("{_id:0, o:{q:0}}", {a: 10, b: 10, c: 10}, {a: 10, b: 10, c: 10});
runPFO("{_id:0, p:{o:{q:0}}}", {a: 10, b: 10, c: 10}, {a: 10, b: 10, c: 10});

// pick non-exist
runPFO("{_id:0, a:{b:1,~} }", {a: 10}, {});
runPFO("{_id:0, a:{b:{c:1,~},~} }", {a: 10}, {});
runPFO("{_id:0, a:{b:{c:1,~},~} }", {a: [1, 2, 3, 4]}, {a: []});
runPFO("{_id:0, a:{b:{c:1,~},~} }", {a: [1, [2, 3], 4]}, {a: [[]]});

// set deep inside
runPFO("{_id:0, a:{b:{c:='X'}} }",
       {a: [1, 2, [3, 4]]},
       {a: [{b: {c: 'X'}}, {b: {c: 'X'}}, [{b: {c: 'X'}}, {b: {c: 'X'}}]]});

runPFO("{_id:0, a:{q:0}}",
       {a: [{b: 1}, {b: 2}, 3, 4, [{b: 5}, {b: 6}], 7, {b: 8}]},
       {a: [{b: 1}, {b: 2}, 3, 4, [{b: 5}, {b: 6}], 7, {b: 8}]});
// drop exist
runPFO("{_id:0, a:{b:0}}",
       {a: [{b: 1}, {b: 2}, 3, 4, [{b: 5}, {b: 6}], 7, {b: 8}]},
       {a: [{}, {}, 3, 4, [{}, {}], 7, {}]});
// set non exist
runPFO("{_id:0, a:{b:{c:=getField(r, 'q')}}}",
       {a: [{b: 1}, {b: 2}, 3, 4, [{b: 5}, {b: 6}], 7, {b: 8}]},
       {a: [{b: {}}, {b: {}}, {b: {}}, {b: {}}, [{b: {}}, {b: {}}], {b: {}}, {b: {}}]});

runPFO("{_id:0, a:{b:{q:1,~},~},~}",
       {
           a: [
               {b: [{c: 10}, {c: 20}, {c: 30}]},
               {b: [{c: 40}, {c: 50}, {c: 60}]},
               {b: [{c: 70}, {c: 80}, {c: 90}]}
           ]
       },
       {a: [{b: [{}, {}, {}]}, {b: [{}, {}, {}]}, {b: [{}, {}, {}]}]});

runPFO("{_id:0, c:0, b:{d:0, e:='X'} }",
       {a: 10, b: {d: 30, e: 40, f: 60}, c: 20},
       {a: 10, b: {e: 'X', f: 60}});
runPFO("{_id:0, c:0, b:{d:0, e:='X'} }", {a: 10, c: 20}, {a: 10, b: {e: 'X'}});
runPFO("{_id:0, c:0, aa:{b:{d:0, e:='X'}} }",
       {a: 10, aa: {b: {d: 30, e: 40, f: 60}, q: 'Y'}, c: 20},
       {a: 10, aa: {b: {e: 'X', f: 60}, q: 'Y'}});
runPFO("{_id:0, c:0, aa:{b:{d:0, e:='X'},~} }",
       {a: 10, aa: {b: {d: 30, e: 40, f: 60}, q: 'Y'}, c: 20},
       {a: 10, aa: {b: {e: 'X', f: 60}}});
runPFO("{_id:0, c:0, aa:{b:{d:0, e:='X'},~} }", {a: 10, c: 20}, {a: 10, aa: {b: {e: 'X'}}});

runPFO("{_id:0, root:{a:{b:='X'}}}",
       {'root': [100, 100, 100]},
       {root: [{a: {b: 'X'}}, {a: {b: 'X'}}, {a: {b: 'X'}}]});
runPFO("{_id:0, root:{a:{b:='X'}}}",
       {'root': {'a': [100, 100, 100]}},
       {root: {a: [{b: 'X'}, {b: 'X'}, {b: 'X'}]}});
}());
