﻿(function () {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/rlp/fts_lib.js");

    db.urd.drop();

    // http://en.wikipedia.org/wiki/Urdu_language
    // lit. "(I) felt happiness (after) meeting you".
    db.urd.insert({ _id: 1, t1: "آپ سے مِل کر خوشی ہوئی۔" });

    db.urd.ensureIndex({ t1: "text" }, { default_language: "urdu" });

    assert.eq(1, db.urd.find( { "$text" : { "$search" : "خوش", "$language" : "urd"} } ).itcount());

    // Positive Term Match
    assert.eq([1], queryIds(db.urd, "خوش"));

    // Negative Term Match
    assert.eq([], queryIds(db.urd, "خوش -س"));

    // Phrase Match
    assert.eq([1], queryIds(db.urd, 'خوش "آپ سے"'));

    // Negative Phrase Match
    //assert.eq([1], queryIds(db.urd, 'كتب -"كَتَبْتُ"'));
}) ();
