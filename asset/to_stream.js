'use strict';

const H = require('highland');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor(/* context */) {
    return function processor(data) {
        let dataArray = data;
        // Handle moving the data array in the case of a full ES response.
        if (data.hits && data.hits.hits) {
            dataArray = data.hits.hits;
        }

        return H(dataArray);
    };
}

function schema() {
    return {
    };
}

module.exports = {
    newProcessor,
    schema,
};
