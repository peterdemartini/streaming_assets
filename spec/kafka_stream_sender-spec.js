'use strict';

/* global describe, it, expect */

const processor = require('../asset/kafka_stream_sender');
const harness = require('teraslice_op_test_harness')(processor);
const Writable = require('stream').Writable;
const _ = require('lodash');

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
            const client = new Writable({
                write(message, encoding, next) {
                    expect(message).not.toBeNull();
                    expect(Buffer.isBuffer(message.value)).toBe(true);
                    expect(message.topic).toEqual('testing');
                    next();
                },
                objectMode: true
            });
            client.producer = {
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
            };

            return { client };
        };

        const stream = H((push) => {
            for (let i = 1; i <= opConfig.size; i += 1) {
                push(null, new StreamEntity(_.sample(inputRecords)));
            }
            stream.end();
        });

        harness.run(stream, opConfig)
            .then((resultStream) => {
                resultStream.toArray((results) => {
                    expect(results.length).toEqual(opConfig.size);

                    // All the results should be the same.
                    expect(results[0] instanceof StreamEntity).toBe(true);
                    expect(results[0].data.host).toContain('example');
                    expect(results[1] instanceof StreamEntity).toBe(true);
                    expect(results[1].data.host).toContain('example');

                    done();
                });
            });
    });
});
