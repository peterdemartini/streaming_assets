'use strict';

const Rx = require('rxjs');

/*
 * Buffer data and return an array.
 */
function newProcessor(context, opConfig) {
    // Use this to immediately flush the buffer so the job can exit..
    const shutdown = Rx.Observable
        .fromEvent(context.foundation.getEventEmitter(), 'worker:shutdown');

    return function processor(incoming) {
        return new Promise((resolve) => {
            const stream = incoming
                .bufferTime(opConfig.wait, null, opConfig.size)
                .takeUntil(shutdown);

            // TODO: this logic might lose any pending buffer at the time of
            // shutdown.
            stream.subscribe(
                resolve,
                () => {},
                () => resolve([])
            );
        });
    };
}

function schema() {
    return {
        size: {
            doc: 'How many records to buffer before a slice is considered complete.',
            default: 10000,
            format: Number
        },
        wait: {
            doc: 'How long to wait for the Buffer to fill up before automatically flushing.',
            default: 30000,
            format: Number
        },
    };
}

module.exports = {
    newProcessor,
    schema,
};
