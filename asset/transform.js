'use strict';

/*
 * Exposes Highland transforms as an operator.
 *
 */
function newProcessor(context, opConfig) {
    // eslint-disable-next-line no-new-func
    const fn = Function(opConfig.args, opConfig.fn);

    return function processor(stream) {
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
