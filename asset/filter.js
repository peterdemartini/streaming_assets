'use strict';

const { isStream } = require('teraslice_stream');
const _ = require('lodash');

/*
 * Exposes the Highland filter operator as an operator.
 */
function newProcessor(context, opConfig) {
    return function processor(stream) {
        if (!isStream(stream)) {
            return stream;
        }
        const { args } = opConfig;
        const fn = opConfig.function;
        const functions = {
            startsWith: record => _.startsWith(_.get(record.data, args.path), args.value),
            random: () => !_.random(0, args.chance || 10)
        };
        if (!_.isFunction(functions[fn])) {
            return Promise.reject(new Error('Not a valid filter function'));
        }
        return stream.filter(functions[fn]);
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
