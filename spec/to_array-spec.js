'use strict';

/* global describe, it, expect */

const processor = require('../asset/to_array');
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

describe('to_array', () => {
    it('should get an array result from an Observable', () => {
        const results = harness.run(H(_.cloneDeep(inputRecords)), opConfig);

        results
            .then((values) => {
                expect(H.isStream(values)).toBeFalsy();
                expect(values.length).toEqual(4);

                expect(values[0].host).toContain('example');
                expect(values[1].host).toContain('example');
                expect(values[2].host).toContain('example');
                expect(values[3].host).toContain('example');
            });
    });

    it('should get an array result from a stream', () => {
        const results = harness.run(H(_.cloneDeep(inputRecords)), opConfig);

        results
            .then((values) => {
                expect(values.length).toEqual(4);

                expect(values[0].host).toContain('example');
                expect(values[1].host).toContain('example');
                expect(values[2].host).toContain('example');
                expect(values[3].host).toContain('example');
            });
    });
});
