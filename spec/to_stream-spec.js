'use strict';

/* global describe, it, expect */

const processor = require('../asset/to_stream');
const harness = require('teraslice_op_test_harness')(processor);

const _ = require('lodash');

const H = require('../asset/node_modules/highland');

const inputRecords = [
    { host: 'example.com' },
    { host: 'www.example.com' },
    { host: 'example.co.uk' },
    { host: 'www.example.co.uk' }
];

const opConfig = {
};

describe('stream', () => {
    it('should generate an empty stream if no input data', () => {
        const results = harness.run([], opConfig);

        expect(H.isStream(results)).toEqual(true);
        results.toArray((values) => {
            expect(values.length).toEqual(0);
        });
    });

    it('should generate a valid stream', () => {
        const results = harness.run(_.cloneDeep(inputRecords), opConfig);

        expect(H.isStream(results)).toEqual(true);

        results.toArray((values) => {
            expect(values.length).toEqual(4);

            expect(values[0].host).toEqual('example.com');
            expect(values[1].host).toEqual('www.example.com');
            expect(values[2].host).toEqual('example.co.uk');
            expect(values[3].host).toEqual('www.example.co.uk');
        });
    });
});
