// Storage Node Watchdog - validate watchdog monitors --logpath
//
load("src/mongo/db/modules/enterprise/jstests/watchdog/lib/wd_test_common.js");

(function() {
    'use strict';

    let control = new CharybdefsControl("logpath_hang");

    const logpath = control.getMountPath();

    testFuseAndMongoD(control, {logpath: logpath + "/foo.log"});

})();
