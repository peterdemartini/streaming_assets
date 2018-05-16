'use strict';

const H = require('highland');
const _ = require('lodash');
const StreamEntity = require('./StreamEntity');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor(/* context */) {
    return function processor(input, sliceLogger) {
        if (H.isStream(input)) {
            sliceLogger.info('input is already a stream');
            return input;
        }
        const dataArray = _.castArray(_.get(input, 'hits.hits', input));

        sliceLogger.info(`converted ${dataArray.length} to a stream`);
        return H(_.map(dataArray, record => new StreamEntity(record)));
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
