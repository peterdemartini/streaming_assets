'use strict';

const H = require('highland');
const _ = require('lodash');

/*
 * Exposes the Highland mapp operator as an operator.
 */
function newProcessor(context, opConfig) {
    return function processor(stream) {
        if (!H.isStream(stream)) {
            return stream;
        }
        const args = opConfig.args;
        const fn = opConfig.function;
        const functions = {
            set: (record) => {
                _.set(record.data, args.path, args.value);
                return record;
            },
            setDate: (record) => {
                const timeFunctions = {
                    now: () => Date.now()
                };
                _.set(record.data, args.path, timeFunctions[args.timeFn]());
                return record;
            }
        };
        return stream.map(functions[fn]);
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
