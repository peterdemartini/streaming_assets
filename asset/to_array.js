'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
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
        return new Promise((resolve, reject) => {
            if (isStream(input)) {
                input.toArray((err, result) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(convertToData(result));
                });
                return;
            }
            resolve(convertToData(_.castArray(input)));
        });
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
