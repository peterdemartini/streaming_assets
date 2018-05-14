'use strict';

/* global describe, it, expect */

const processor = require('../asset/map');
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
    args: 'x',
    fn: 'x.host += "-test"; return x;'
};

describe('map', () => {
    it('should map each input value', () => {
        const results = harness.run(H(_.cloneDeep(inputRecords)), opConfig);

        results.toArray((values) => {
            expect(values.length).toEqual(4);

            expect(values[0].host).toEqual('example.com-test');
            expect(values[1].host).toEqual('www.example.com-test');
            expect(values[2].host).toEqual('example.co.uk-test');
            expect(values[3].host).toEqual('www.example.co.uk-test');
        });
    });
});
