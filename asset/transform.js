'use strict';

const H = require('highland');
/*
 * Exposes Highland transforms as an operator.
 *
 */
function newProcessor(context, opConfig) {
    // eslint-disable-next-line no-new-func
    const fn = Function(opConfig.args, opConfig.fn);

    return function processor(stream, sliceLogger) {
        if (!H.isStream(stream)) {
            sliceLogger.warn('input is not a stream to transform');
            return stream;
        }
        const forked = stream.fork();
        if (opConfig.fn) {
            return forked[opConfig.tx](fn);
        } else if (opConfig.obj) {
            return forked[opConfig.tx](opConfig.obj);
        }

        return forked[opConfig.tx]();
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
