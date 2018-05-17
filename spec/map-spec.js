'use strict';

/* global describe, it, expect */

const processor = require('../asset/map');
const harness = require('teraslice_op_test_harness')(processor);
const StreamEntity = require('../asset/StreamEntity');

const _ = require('lodash');

const H = require('../asset/node_modules/highland');

const inputRecords = [
    { host: 'example.com' },
    { host: 'www.example.com' },
    { host: 'example.co.uk' },
    { host: 'www.example.co.uk' }
];

describe('map', () => {
    describe('when a stream is given', () => {
        it('should map add processed to each record', () => {
            const opConfig = {
                function: 'set',
                args: {
                    path: ['processed'],
                    value: true
                }
            };

            const streamRecords = _.map(inputRecords, record => new StreamEntity(_.cloneDeep(record)));
            const results = harness.run(H(streamRecords), opConfig);

            results.toArray((values) => {
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
            });
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
            const streamRecords = _.map(inputRecords, record => new StreamEntity(_.cloneDeep(record)));
            const results = harness.run(H(streamRecords), opConfig);

            results.toArray((values) => {
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

            const results = harness.run(_.cloneDeep(inputRecords), opConfig);

            results.forEach((values) => {
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
            const results = harness.run(_.cloneDeep(inputRecords), opConfig);

            results.forEach((values) => {
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
        });
    });
});
