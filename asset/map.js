'use strict';

/*
 * Exposes the Highland map operator as an operator.
 *
 */
function newProcessor(context, opConfig) {
    // eslint-disable-next-line no-new-func
    const fn = Function(opConfig.args, opConfig.fn);

    return function processor(stream) {
        return stream.map(fn);
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
