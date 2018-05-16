'use strict';

const H = require('highland');
const _ = require('lodash');
const Promise = require('bluebird');
const StreamEntity = require('./StreamEntity');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor() {
    return function processor(input) {
        const convertToData = records => _.map(records, (record) => {
            if (record instanceof StreamEntity) {
                return record.data;
            }
            return record;
        });
        return new Promise((resolve) => {
            if (H.isStream(input)) {
                input.toArray((result) => {
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
