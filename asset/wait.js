'use strict';

/*
 * This processor will wait for a given period of time before resolving
 */

function newProcessor(context, opConfig) {
    const events = context.foundation.getEventEmitter();
    return function processor(input) {
        return new Promise((resolve) => {
            let timeoutId = null;
            const shutdown = () => {
                events.removeListener('worker:shutdown', shutdown);
                clearTimeout(timeoutId);
                resolve(input);
            };
            timeoutId = setTimeout(() => {
                events.removeListener('worker:shutdown', shutdown);
                resolve(input);
            }, opConfig.ms);
            events.on('worker:shutdown', shutdown);
        });
    };
}

function schema() {
    return {
        ms: {
            doc: 'How long to wait in milliseconds',
            default: 1000,
            format: Number
        },
    };
}

module.exports = {
    newProcessor,
    schema
};
