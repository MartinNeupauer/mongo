//
// Utility functions for integration testing slot-based execution.
//

load("jstests/aggregation/extras/utils.js");  // For arrayEq and orderedArrayEq.

/**
 * Sets the knob for enabling/disabling SBE to the value of 'isSBE'.
 */
function toggleSBE(isSBE) {
    assert.commandWorked(
        db.adminCommand({setParameter: 1, "internalQueryEnableSlotBasedExecutionEngine": isSBE}));
}

/**
 * Asserts that the given cursor-generating command (e.g. find or agg) returns the same results with
 * SBE on and off. If 'expectSort' is true, then also verifies that the result sets are in the same
 * order. Otherwise, the result sets are treated as unordered.
 */
function testCursorCommandSBE(testDb, command, expectSort) {
    const runCursorCommand = function(testDb, command) {
        const firstBatch = testDb.runCommand(command);
        const cursor = new DBCommandCursor(testDb, firstBatch);
        return cursor.toArray();
    };

    toggleSBE(true);
    const resultsWithSBE = runCursorCommand(testDb, command);
    toggleSBE(false);
    const resultsWithDefault = runCursorCommand(testDb, command);

    if (expectSort) {
        assert(orderedArrayEq(resultsWithSBE, resultsWithDefault),
               `actual=${tojson(resultsWithSBE)}, expected=${tojson(resultsWithDefault)}, query=${
                   tojson(command)}`);
    } else {
        assert(arrayEq(resultsWithSBE, resultsWithDefault),
               `actual=${tojson(resultsWithSBE)}, expected=${tojson(resultsWithDefault)}, query=${
                   tojson(command)}`);
    }
}
