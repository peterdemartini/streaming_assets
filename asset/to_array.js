'use strict';

const H = require('highland');
const _ = require('lodash');
const Promise = require('bluebird');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor() {
    return function processor(input) {
        return new Promise((resolve) => {
            if (H.isStream(input)) {
                input.toArray((result) => {
                    resolve(result);
                });
                return;
            }
            resolve(_.castArray(input));
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
