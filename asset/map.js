'use strict';

const H = require('highland');
const _ = require('lodash');

/*
 * Exposes the Highland mapp operator as an operator.
 */
function newProcessor(context, opConfig) {
    return function processor(stream) {
        const args = opConfig.args;
        const fn = opConfig.function;
        const functions = {
            set: (data) => {
                _.set(data, args.path, args.value);
                return data;
            },
            setDate: (data) => {
                const timeFunctions = {
                    now: () => Date.now()
                };
                _.set(data, args.path, timeFunctions[args.timeFn]());
                return data;
            }
        };
        if (!_.isFunction(functions[fn])) {
            return Promise.reject(new Error('Not a valid map function'));
        }
        if (H.isStream(stream)) {
            return stream.map((record) => {
                record.data = functions[fn](record.data);
                return record;
            });
        }
        return _.map(functions[fn]);
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
