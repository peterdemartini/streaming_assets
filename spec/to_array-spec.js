'use strict';

const processor = require('../asset/to_array');
const harness = require('teraslice_op_test_harness')(processor);

const _ = require('lodash');

const Rx = require('../asset/node_modules/rxjs');
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
        const results = harness.run(
            Rx.Observable.from(_.cloneDeep(inputRecords)), opConfig
        );

        results
            .then((values) => {
                expect(values.length).toEqual(4);

                expect(values[0].host).toEqual('example.com');
                expect(values[1].host).toEqual('www.example.com');
                expect(values[2].host).toEqual('example.co.uk');
                expect(values[3].host).toEqual('www.example.co.uk');
            });
    });

    it('should get an array result from a stream', () => {
        const results = harness.run(H(_.cloneDeep(inputRecords)), opConfig);

        results
            .then((values) => {
                expect(values.length).toEqual(4);

                expect(values[0].host).toEqual('example.com');
                expect(values[1].host).toEqual('www.example.com');
                expect(values[2].host).toEqual('example.co.uk');
                expect(values[3].host).toEqual('www.example.co.uk');
            });
    });
});
