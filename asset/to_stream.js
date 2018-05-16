'use strict';

const H = require('highland');
const StreamEntity = require('./StreamEntity');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor(/* context */) {
    return function processor(data, sliceLogger) {
        if (H.isStream(data)) {
            sliceLogger.info('input is already a stream');
            return data;
        }
        let dataArray = data;
        // Handle moving the data array in the case of a full ES response.
        if (data.hits && data.hits.hits) {
            dataArray = data.hits.hits;
        }

        sliceLogger.info(`converted ${dataArray.length} to a stream`);
        return H(dataArray).map(record => new StreamEntity(record, null, new Date()));
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
