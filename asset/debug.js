'use strict';

/*
 * This teraslice process debugs incoming data and pass it along
 */

const util = require('util');

function newProcessor() {
    return function processor(input, sliceLogger) {
        sliceLogger.info(`debug_processor: type: ${typeof input}, value: ${util.inspect(input)}`);
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
