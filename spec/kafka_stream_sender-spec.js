'use strict';

const processor = require('../asset/kafka_stream_sender');
const harness = require('teraslice_op_test_harness')(processor);

const _ = require('lodash');

const CheckpointEntity = require('../asset/CheckpointEntity');
const StreamEntity = require('../asset/StreamEntity');

// const Rx = require('../asset/node_modules/rxjs');
const H = require('../asset/node_modules/highland');

const inputRecords = [
    { host: 'example.com' },
    { host: 'www.example.com' },
    { host: 'example.co.uk' },
    { host: 'www.example.co.uk' }
];

const opConfig = {
    topic: 'testing',
    size: 500
};

describe('kafka_stream_sender', () => {
    it('should properly batch records', (done) => {
        // Mock out the context apis
        harness.context.foundation.getConnection = (config) => {
            expect(config.type).toEqual('kafka');

            return {
                client: {
                    state: 0,
                    // Produce is called once for every record.
                    produce() { this.state += 1; },
                    flush(timeout, cb) {
                        // Flush should be called on opConfig.size boundaries
                        expect(this.state % opConfig.size === 0).toBe(true);
                        cb();
                    },
                    on(target, fn) { fn(); /* Immediately trigger producer ready */ },
                    removeListener() {}
                }
            };
        };

        const records = [];

        for (let i = 1; i <= 10000; i += 1) {
            records.push(new StreamEntity(inputRecords[0]));
            if (i % opConfig.size === 0) {
                records.push(new CheckpointEntity(1));
            }
        }

        // harness.run(Rx.Observable.from(records), opConfig)
        harness.run(H(records), opConfig)
            .then((results) => {
                results.toArray((values) => {
                // results.toArray().subscribe((values) => {
                    expect(values.length).toEqual(10020);

                    // All the results should be the same.
                    expect(values[0] instanceof StreamEntity).toBe(true);
                    expect(values[0].data.host).toEqual('example.com');
                    expect(values[1] instanceof StreamEntity).toBe(true);
                    expect(values[1].data.host).toEqual('example.com');

                    expect(values[499] instanceof StreamEntity).toBe(true);
                    expect(values[501] instanceof StreamEntity).toBe(true);
                    expect(values[1000] instanceof StreamEntity).toBe(true);

                    expect(values[500] instanceof CheckpointEntity).toBe(true);
                    expect(values[1001] instanceof CheckpointEntity).toBe(true);

                    done();
                });
            });
    });
});
