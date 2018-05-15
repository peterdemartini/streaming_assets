'use strict';

const H = require('highland');
const Promise = require('bluebird');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor() {
    return function processor(input) {
        if (H.isStream(input)) {
            return input.collect().toPromise(Promise);
        }
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
