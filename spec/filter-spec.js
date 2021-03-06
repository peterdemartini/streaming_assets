'use strict';

/* global describe, it, expect */

const processor = require('../asset/filter');
const harness = require('teraslice_op_test_harness')(processor);
const { StreamEntity, Stream } = require('../asset/node_modules/teraslice_stream');

const _ = require('lodash');

const inputRecords = [
    { host: 'example.com' },
    { host: 'www.example.com' },
    { host: 'example.co.uk' },
    { host: 'www.example.co.uk' }
];

describe('map', () => {
    it('should filter out the records that does not start with www', (done) => {
        const opConfig = {
            args: {
                path: ['host'],
                value: 'www'
            },
            function: 'startsWith'
        };

        const records = _.map(inputRecords, record => new StreamEntity(_.cloneDeep(record)));
        const stream = new Stream();
        stream.write(records).then(() => stream.end());
        const results = harness.run(stream, opConfig);

        results.toArray().then((values) => {
            expect(values.length).toEqual(2);
            expect(values[0] instanceof StreamEntity).toEqual(true);
            expect(values[0].data.host).toEqual('www.example.com');
            expect(values[1].data.host).toEqual('www.example.co.uk');
            done();
        }).catch(done.fail);
    });
    it('should filter out some of the records', (done) => {
        const opConfig = {
            args: {
                chance: 1
            },
            function: 'random'
        };
        const records = _.times(1000, () => _.sample(inputRecords));
        const streamRecords = _.map(records, record => new StreamEntity(_.cloneDeep(record)));
        const stream = new Stream();
        stream.write(streamRecords).then(() => stream.end());
        const results = harness.run(stream, opConfig);

        results.toArray().then((values) => {
            expect(values.length).toBeLessThan(_.size(records));
            expect(values[0] instanceof StreamEntity).toEqual(true);
            expect(values[0].data.host).toContain('example');
            expect(values[1].data.host).toContain('example');
            done();
        }).catch(done.fail);
    });
});
