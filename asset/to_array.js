'use strict';

const H = require('highland');
const Promise = require('bluebird');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor() {
    return function processor(stream) {
        if (H.isStream(stream)) {
            return stream.collect().toPromise(Promise);
        }
        return Promise.resolve();
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
