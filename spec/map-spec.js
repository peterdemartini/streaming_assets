'use strict';

/* global describe, it, expect */

const processor = require('../asset/map');
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
    describe('when a stream is given', () => {
        it('should map add processed to each record', (done) => {
            const opConfig = {
                function: 'set',
                args: {
                    path: ['processed'],
                    value: true
                }
            };

            const newEntity = record => new StreamEntity(_.cloneDeep(record));
            const streamRecords = _.map(inputRecords, newEntity);
            const results = harness.run(new Stream(streamRecords), opConfig);

            results.toArray((err, values) => {
                if (err) {
                    done(err);
                    return;
                }
                expect(values.length).toEqual(4);

                expect(values[0] instanceof StreamEntity).toBeTruthy();
                expect(values[0].data.host).toEqual('example.com');
                expect(values[1].data.host).toEqual('www.example.com');
                expect(values[2].data.host).toEqual('example.co.uk');
                expect(values[3].data.host).toEqual('www.example.co.uk');
                expect(values[0].data.processed).toBeTruthy();
                expect(values[1].data.processed).toBeTruthy();
                expect(values[2].data.processed).toBeTruthy();
                expect(values[3].data.processed).toBeTruthy();
                done();
            });
        });
        it('should add a new date on processedAt', (done) => {
            const opConfig = {
                function: 'setDate',
                args: {
                    path: ['processedAt'],
                    timeFn: 'now'
                }
            };
            const startRange = Date.now() - 1;
            const newEntity = record => new StreamEntity(_.cloneDeep(record));
            const streamRecords = _.map(inputRecords, newEntity);
            const results = harness.run(new Stream(streamRecords), opConfig);

            results.toArray((err, values) => {
                if (err) {
                    done(err);
                    return;
                }
                expect(values.length).toEqual(4);

                expect(values[0] instanceof StreamEntity).toBeTruthy();
                expect(values[0].data.host).toEqual('example.com');
                expect(values[1].data.host).toEqual('www.example.com');
                expect(values[2].data.host).toEqual('example.co.uk');
                expect(values[3].data.host).toEqual('www.example.co.uk');
                const endRange = Date.now() + 1;
                expect(_.inRange(values[0].data.processedAt, startRange, endRange)).toBeTruthy();
                expect(_.inRange(values[1].data.processedAt, startRange, endRange)).toBeTruthy();
                expect(_.inRange(values[2].data.processedAt, startRange, endRange)).toBeTruthy();
                expect(_.inRange(values[3].data.processedAt, startRange, endRange)).toBeTruthy();
                done();
            });
        });
        it('should be JSON parsable', (done) => {
            const opConfig = {
                function: 'JSONParse'
            };
            const newEntity = record => new StreamEntity(JSON.stringify(record));
            const streamRecords = _.map(inputRecords, newEntity);
            const results = harness.run(new Stream(streamRecords), opConfig);

            results.toArray((err, values) => {
                if (err) {
                    done(err);
                    return;
                }
                expect(values.length).toEqual(4);

                expect(values[0] instanceof StreamEntity).toBeTruthy();
                expect(values[0].data.host).toEqual('example.com');
                expect(values[1].data.host).toEqual('www.example.com');
                expect(values[2].data.host).toEqual('example.co.uk');
                expect(values[3].data.host).toEqual('www.example.co.uk');
                done();
            });
        });
        it('should be JSON parsable with it is buffer', (done) => {
            const opConfig = {
                function: 'JSONParse'
            };
            const newEntity = record => new StreamEntity(Buffer.from(JSON.stringify(record)));
            const streamRecords = _.map(inputRecords, newEntity);
            const results = harness.run(new Stream(streamRecords), opConfig);
            results.toArray((err, values) => {
                if (err) {
                    done(err);
                    return;
                }
                expect(values.length).toEqual(4);

                expect(values[0] instanceof StreamEntity).toBeTruthy();
                expect(values[0].data.host).toEqual('example.com');
                expect(values[1].data.host).toEqual('www.example.com');
                expect(values[2].data.host).toEqual('example.co.uk');
                expect(values[3].data.host).toEqual('www.example.co.uk');
                done();
            });
        });
        it('should be JSON stringifyable', (done) => {
            const opConfig = {
                function: 'JSONStringify'
            };
            const newEntity = record => new StreamEntity(_.cloneDeep(record));
            const streamRecords = _.map(inputRecords, newEntity);
            const results = harness.run(new Stream(streamRecords), opConfig);

            results.toArray((err, values) => {
                if (err) {
                    done(err);
                    return;
                }
                expect(values.length).toEqual(4);

                expect(values[0] instanceof StreamEntity).toBeTruthy();
                expect(typeof values[0].data).toEqual('string');
                expect(typeof values[1].data).toEqual('string');
                expect(typeof values[2].data).toEqual('string');
                expect(typeof values[3].data).toEqual('string');
                done();
            });
        });
    });
    describe('when a array is given', () => {
        it('should map add processed to each record', () => {
            const opConfig = {
                function: 'set',
                args: {
                    path: ['processed'],
                    value: true
                }
            };

            const values = harness.run(_.cloneDeep(inputRecords), opConfig);

            expect(values.length).toEqual(4);

            expect(values[0] instanceof StreamEntity).toBeFalsy();
            expect(values[0].host).toEqual('example.com');
            expect(values[1].host).toEqual('www.example.com');
            expect(values[2].host).toEqual('example.co.uk');
            expect(values[3].host).toEqual('www.example.co.uk');
            expect(values[0].processed).toBeTruthy();
            expect(values[1].processed).toBeTruthy();
            expect(values[2].processed).toBeTruthy();
            expect(values[3].processed).toBeTruthy();
        });
        it('should add a new date on processedAt', () => {
            const opConfig = {
                function: 'setDate',
                args: {
                    path: ['processedAt'],
                    timeFn: 'now'
                }
            };
            const startRange = Date.now() - 1;
            const values = harness.run(_.cloneDeep(inputRecords), opConfig);

            expect(values.length).toEqual(4);

            expect(values[0] instanceof StreamEntity).toBeFalsy();
            expect(values[0].host).toEqual('example.com');
            expect(values[1].host).toEqual('www.example.com');
            expect(values[2].host).toEqual('example.co.uk');
            expect(values[3].host).toEqual('www.example.co.uk');
            const endRange = Date.now() + 1;
            expect(_.inRange(values[0].processedAt, startRange, endRange)).toBeTruthy();
            expect(_.inRange(values[1].processedAt, startRange, endRange)).toBeTruthy();
            expect(_.inRange(values[2].processedAt, startRange, endRange)).toBeTruthy();
            expect(_.inRange(values[3].processedAt, startRange, endRange)).toBeTruthy();
        });
        it('should be JSON parsable', () => {
            const opConfig = {
                function: 'JSONParse'
            };
            const records = _.map(inputRecords, record => JSON.stringify(record));
            const values = harness.run(records, opConfig);

            expect(values.length).toEqual(4);

            expect(values[0] instanceof StreamEntity).toBeFalsy();
            expect(values[0].host).toEqual('example.com');
            expect(values[1].host).toEqual('www.example.com');
            expect(values[2].host).toEqual('example.co.uk');
            expect(values[3].host).toEqual('www.example.co.uk');
        });
        it('should be JSON parsable with it is buffer', () => {
            const opConfig = {
                function: 'JSONParse'
            };
            const records = _.map(inputRecords, record => Buffer.from(JSON.stringify(record)));
            const values = harness.run(records, opConfig);

            expect(values.length).toEqual(4);

            expect(values[0] instanceof StreamEntity).toBeFalsy();
            expect(values[0].host).toEqual('example.com');
            expect(values[1].host).toEqual('www.example.com');
            expect(values[2].host).toEqual('example.co.uk');
            expect(values[3].host).toEqual('www.example.co.uk');
        });
        it('should be JSON stringifyable', () => {
            const opConfig = {
                function: 'JSONStringify'
            };
            const values = harness.run(_.cloneDeep(inputRecords), opConfig);

            expect(values.length).toEqual(4);

            expect(values[0] instanceof StreamEntity).toBeFalsy();
            expect(typeof values[0]).toEqual('string');
            expect(typeof values[1]).toEqual('string');
            expect(typeof values[2]).toEqual('string');
            expect(typeof values[3]).toEqual('string');
        });
    });
});
