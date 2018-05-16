'use strict';

const H = require('highland');
/*
 * Exposes Highland transforms as an operator.
 *
 */
function newProcessor(context, opConfig) {
    // eslint-disable-next-line no-new-func
    const fn = Function(opConfig.args, opConfig.fn);

    return function processor(input) {
        const stream = H.isStream(input) ? input.fork() : H(input);
        if (opConfig.fn) {
            return stream[opConfig.tx](fn);
        } else if (opConfig.obj) {
            return stream[opConfig.tx](opConfig.obj);
        }

        return stream[opConfig.tx]();
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
