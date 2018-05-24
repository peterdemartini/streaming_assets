'use strict';

/* global describe, it, expect */

const processor = require('../asset/kafka_stream_sender');
const harness = require('teraslice_op_test_harness')(processor);
const _ = require('lodash');

const { StreamEntity, Stream } = require('../asset/node_modules/teraslice_stream');

const H = require('../asset/node_modules/highland');

const inputRecords = [
    { host: 'example.com' },
    { host: 'www.example.com' },
    { host: 'example.co.uk' },
    { host: 'www.example.co.uk' }
];

const opConfig = {
    topic: 'testing',
    size: 50,
    continue_stream: true
};

describe('kafka_stream_sender', () => {
    it('should properly batch records', (done) => {
    // Mock out the context apis
        harness.context.foundation.getConnection = (config) => {
            expect(config.type).toEqual('kafka');

            return {
                client: {
                    isConnected() { return true; },
                    state: 0,
                    // Produce is called once for every record.
                    produce() {
                        this.state += 1;
                        if (this.state > opConfig.size) {
                            return new Error('Queue full');
                        }
                        return true;
                    },
                    flush(timeout, cb) {
                        this.state = 0;
                        // Flush should be called on opConfig.size boundaries
                        expect(this.state % opConfig.size === 0).toBe(true);
                        setTimeout(cb, 100);
                    },
                    on(target, fn) { fn(); /* Immediately trigger producer ready */ },
                    removeListener() {}
                }
            };
        };

        const inputSize = 1000;
        const stream = H((push) => {
            const endStream = _.after(inputSize, () => {
                stream.end();
            });
            _.times(inputSize, (i) => {
                const record = new StreamEntity(_.sample(inputRecords), { key: i });
                const send = () => {
                    if (stream.paused) {
                        _.delay(send, 1);
                        return;
                    }
                    push(null, record);
                    endStream();
                };
                _.delay(send, i * 2);
            });
        });

        harness.run(new Stream(stream), opConfig)
            .then((resultStream) => {
                resultStream.toArray((err, results) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    expect(results.length).toEqual(inputSize);

                    // All the results should be the same.
                    expect(results[0] instanceof StreamEntity).toBe(true);
                    expect(results[0].data.host).toContain('example');
                    expect(results[1] instanceof StreamEntity).toBe(true);
                    expect(results[1].data.host).toContain('example');

                    done();
                });
            });
    }, 5000);
});
