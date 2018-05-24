'use strict';

/* global describe, it, expect */

const processor = require('../asset/to_stream');
const harness = require('teraslice_op_test_harness')(processor);
const { StreamEntity, isStream } = require('teraslice_stream');

const _ = require('lodash');

const inputRecords = [
    { host: 'example.com' },
    { host: 'www.example.com' },
    { host: 'example.co.uk' },
    { host: 'www.example.co.uk' }
];

const opConfig = {
};

describe('to_stream', () => {
    it('should generate an empty stream if no input data', (done) => {
        const results = harness.run([], opConfig);

        expect(isStream(results)).toEqual(true);
        results.toArray((err, values) => {
            if (err) {
                done(err);
                return;
            }
            expect(values.length).toEqual(0);
            done();
        });
    });

    it('should generate a valid stream', (done) => {
        const results = harness.run(_.cloneDeep(inputRecords), opConfig);

        expect(isStream(results)).toEqual(true);

        results.toArray((err, values) => {
            if (err) {
                done(err);
                return;
            }
            expect(values.length).toEqual(4);

            expect(values[0].toBuffer()).toEqual(Buffer.from(JSON.stringify(values[0].data)));
            expect(values[0] instanceof StreamEntity).toBe(true);
            expect(values[0].data.host).toContain('example');
            expect(values[1] instanceof StreamEntity).toBe(true);
            expect(values[1].data.host).toContain('example');
            expect(values[2] instanceof StreamEntity).toBe(true);
            expect(values[2].data.host).toContain('example');
            expect(values[3] instanceof StreamEntity).toBe(true);
            expect(values[3].data.host).toContain('example');
            done();
        });
    });
});
