'use strict';

const Promise = require('bluebird');
const Rx = require('rxjs');

const CheckpointEntity = require('./CheckpointEntity');
const StreamEntity = require('./StreamEntity');

function newProcessor(context, opConfig) {
    let producerReady = false;

    const bufferSize = 5 * opConfig.size;

    const producer = context.foundation.getConnection({
        type: 'kafka',
        endpoint: opConfig.connection,
        options: {
            type: 'producer'
        },
        rdkafka_options: {
            'compression.codec': opConfig.compression,
            'queue.buffering.max.messages': bufferSize,
            'queue.buffering.max.ms': opConfig.wait,
            'batch.num.messages': opConfig.size,
            'topic.metadata.refresh.interval.ms': opConfig.metadata_refresh,
            'log.connection.close': false
        }
    }).client;

    producer.on('ready', () => {
        producerReady = true;
    });

    // Use this to immediately flush the buffer so the job can exit..
    const shutdown = Rx.Observable
        .fromEvent(context.foundation.getEventEmitter(), 'worker:shutdown');

    let configured = false;
    let stream;
    let response;
    let checkpoints;
    let count = 0;

    return function processor(incoming) {
        return new Promise((resolve, reject) => {
            // TODO: this logic might lose any pending buffer at the time of
            // shutdown.
            if (!configured) {
                configured = true;

                // We return an observer of the stream so as not to impose
                // backpressure.
                response = incoming.observe();

                let bufferCount = 0;

                // Pull off a separate stream of checkpoint entries so we can know
                // when to flush.
                checkpoints = incoming
                    .fork()
                    .filter(record => record instanceof CheckpointEntity)
                    //.tap(console.log)
                    //.each(record => console.log(record));

                stream = incoming
                    .fork()
                    .filter(record => record instanceof StreamEntity)
                    .batchWithTimeOrCount(opConfig.wait, opConfig.size)
                    .each(records => send(records));
                    //.takeUntil(shutdown)
                    // This buffer should now be whatever intermediate size we want.
                    //.bufferCount(opConfig.size)
                    //.bufferTime(opConfig.wait, null, opConfig.size)
                    /*.bufferWhen(Rx.Observable.race(
                        Rx.Observable.interval(opConfig.wait),
                        checkpoints
                    ))*/
                    //.do((record) => bufferCount += 1)
                    //.bufferTime(opConfig.wait, null, opConfig.size)
                    //.bufferCount(opConfig.size)
                    // .do(() => console.log("BUFFER flushing"))
                    //.window(checkpoints)
                    //.mergeAll()
                    //.bufferTime(opConfig.wait, null, opConfig.size)

                /* stream.subscribe(
                    (records) => { send(records); },
                    () => {},
                    () => { console.log('Stream completed', bufferCount); }
                ); */
            }

            checkpoints
                .tap(console.log)
                .each(() => flush(true));

            /* checkpoints.subscribe(
                () => { flush(true); },
                () => {},
                () => { flush(false); }
            ); */

            function error(err) {
                reject(err);
            }

            function flush(checkpointing) {
                // TODO: this flush timeout may need to be configurable
                producer.flush(60000, (err) => {
                    // Remove the error listener so they don't accrue across slices.
                    producer.removeListener('event.error', error);
console.log("flushing " + count + " " + checkpointing);
                    if (err) {
                        reject(err);
                        return;
                    }

                    // TODO: this will switch to being driven by checkpoints
                    if (checkpointing) {
                        resolve(response);
                    }
                });
            }

            function send(records) {
                if (producerReady) {
                    for (let i = 0; i < records.length; i += 1) {
                        const record = records[i];

                        count += 1;
if (count % 1000 === 0) console.log("Processed for send: ", count);
                        producer.produce(
                            opConfig.topic,
                            // This is the partition. There may be use cases where
                            // we'll need to control this.
                            null,
                            new Buffer(JSON.stringify(record.data)),
                            record.key, // key
                            record.processTime // timestamp ... which one to use here?
                        );
                    }

                    flush(false);
                }
            }
        });
    };
}


function schema() {
    return {
        topic: {
            doc: 'Name of the Kafka topic to send data to',
            default: '',
            format: 'required_String'
        },
        connection: {
            doc: 'The Kafka producer connection to use.',
            default: 'default',
            format: String
        },
        compression: {
            doc: 'Type of compression to use',
            default: 'gzip',
            format: ['none', 'gzip', 'snappy', 'lz4']
        },
        wait: {
            doc: 'How long to wait for `size` messages to become available on the producer.',
            default: 5000,
            format: Number
        },
        size: {
            doc: 'How many messages will be batched and sent to kafka together.',
            default: 10000,
            format: Number
        },
        metadata_refresh: {
            doc: 'How often the producer will poll the broker for metadata information. Set to -1 to disable polling.',
            default: 300000,
            format: Number
        }
    };
}

function checkpointAware() {
    return true;
}

module.exports = {
    newProcessor,
    checkpointAware,
    schema
};
