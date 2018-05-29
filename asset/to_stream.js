'use strict';

const _ = require('lodash');
const { Stream, StreamEntity, isStream } = require('teraslice_stream');
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
        const records = _.map(dataArray, record => new StreamEntity(record));
        const stream = new Stream();
        stream.write(records)
            .then(() => stream.end())
            .catch(err => sliceLogger.warn('unable to write to stream', err));
        return stream;
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
