'use strict';

const H = require('highland');
const _ = require('lodash');

/*
 * Exposes the Highland map processor
 */
function newProcessor(context, opConfig) {
    return function processor(stream, sliceLogger) {
        const args = opConfig.args;
        const fn = opConfig.function;
        const functions = {
            set: (data) => {
                _.set(data, args.path, args.value);
                return data;
            },
            JSONStringify: (data) => {
                if (_.isBuffer(data)) {
                    sliceLogger.warn('data is not a JSON stringifyable');
                    return data;
                }
                try {
                    return JSON.stringify(data);
                } catch (err) {
                    sliceLogger.warn('json stringify error', err);
                    return data;
                }
            },
            JSONParse: (input) => {
                const data = _.isBuffer(input) ? input.toString() : input;
                if (!_.isString(data)) {
                    sliceLogger.warn('data is not a JSON parseable');
                    return data;
                }
                try {
                    return JSON.parse(data);
                } catch (err) {
                    sliceLogger.warn('json parse error', err);
                    return data;
                }
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
        return _.map(stream, functions[fn]);
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
