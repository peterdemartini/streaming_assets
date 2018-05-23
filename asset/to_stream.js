'use strict';

const _ = require('lodash');
const { StreamEntity, Stream, isStream } = require('teraslice-stream');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor(/* context */) {
    return function processor(input, sliceLogger) {
        if (isStream(input)) {
            sliceLogger.info('to_stream input is already a stream');
            return input;
        }
        const dataArray = _.castArray(_.get(input, 'hits.hits', input));

        sliceLogger.info(`converted ${dataArray.length} to a stream`);
        return new Stream(_.map(dataArray, record => new StreamEntity(record)));
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
