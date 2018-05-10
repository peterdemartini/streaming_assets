'use strict';

const Rx = require('rxjs');

/*
 * Buffer data and return an array.
 */
function newProcessor(context, opConfig) {
    // Use this to immediately flush the buffer so the job can exit..
    const shutdown = Rx.Observable
        .fromEvent(context.foundation.getEventEmitter(), 'worker:shutdown');

    let configured = false;
    let stream;
    let count = 0;

    return function processor(incoming) {
        if (!configured) {
            configured = true;
            stream = incoming
                .bufferTime(opConfig.wait, null, opConfig.size)
                .takeUntil(shutdown);

            //stream = incoming
            //    .batchWithTimeOrCount(opConfig.wait, opConfig.size)
        }
        count++;
        return new Promise((resolve) => {
console.log('Promise pending resolution')

// This may be returning the same batch everytime due to resolve being called more
// than once.

            //stream.fork().each(resolve);

            // TODO: this logic might lose any pending buffer at the time of
            // shutdown.
            stream.subscribe(
                () => {
                    resolve();
                    console.log("Resolving slice ", count);
                },
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
