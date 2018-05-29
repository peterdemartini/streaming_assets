'use strict';

const _ = require('lodash');
const { isStream, isStreamEntity } = require('teraslice_stream');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor() {
    return function processor(input) {
        const convertToData = records => _.map(records, (record) => {
            if (isStreamEntity(record)) {
                return record.toJSON();
            }
            return record;
        });
        if (isStream(input)) {
            return input.toArray().then(convertToData);
        }
        return Promise.resolve(convertToData(_.castArray(input)));
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
