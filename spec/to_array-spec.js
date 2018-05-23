'use strict';

/* global describe, it, expect */

const processor = require('../asset/to_array');
const harness = require('teraslice_op_test_harness')(processor);
const { StreamEntity, isStream } = require('teraslice-stream');

const _ = require('lodash');

const inputRecords = [
    { host: 'example.com' },
    { host: 'www.example.com' },
    { host: 'example.co.uk' },
    { host: 'www.example.co.uk' }
];

const opConfig = {
};

describe('to_array', () => {
    it('should get handle a normal array', () => {
        const results = harness.run(_.cloneDeep(inputRecords), opConfig);

        results
            .then((values) => {
                expect(isStream(values)).toBeFalsy();
                expect(values.length).toEqual(4);

                expect(values[0].host).toContain('example');
                expect(values[1].host).toContain('example');
                expect(values[2].host).toContain('example');
                expect(values[3].host).toContain('example');
            });
    });

    it('should get an array result from a stream', () => {
        const streamRecords = _.map(inputRecords, record => new StreamEntity(_.cloneDeep(record)));
        const results = harness.run(streamRecords, opConfig);

        return results.then((values) => {
            expect(values.length).toEqual(4);

            expect(values[0].host).toContain('example');
            expect(values[1].host).toContain('example');
            expect(values[2].host).toContain('example');
            expect(values[3].host).toContain('example');
        });
    });
});
