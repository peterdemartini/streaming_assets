'use strict';

const Rx = require('rxjs');
/*
 * This processor adapts the incoming array into a Highland stream so that
 * downstream processors can work on the stream.
 */
function newProcessor(/* context */) {
    return function processor(incoming) {
        return new Promise((resolve) => {
            if (incoming instanceof Rx.Observable) {
                incoming
                    .toArray()
                    .subscribe(resolve);
            } else {
                incoming.toArray(resolve);
            }
        });
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
