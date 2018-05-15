'use strict';

/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */

const util = require('util');

function newProcessor() {
    return function processor(input, sliceLogger) {
        sliceLogger.debug(typeof input);
        sliceLogger.debug(util.inspect(input));
        return Promise.resolve(input);
    };
}

function schema() {
    return {
    };
}

module.exports = {
    newProcessor,
    schema
};
