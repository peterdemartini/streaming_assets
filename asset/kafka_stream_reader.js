'use strict';

const Promise = require('bluebird');
const _ = require('lodash');

const Rx = require('rxjs');
const H = require('highland');

const CheckpointEntity = require('./CheckpointEntity');
const StreamEntity = require('./StreamEntity');

const KAFKA_NO_OFFSET_STORED = -168;

function newReader(context, opConfig) {
    const events = context.foundation.getEventEmitter();
    const jobLogger = context.logger;

    function createConsumer() {
        return context.foundation.getConnection({
            type: 'kafka',
            endpoint: opConfig.connection,
            options: {
                type: 'consumer',
                group: opConfig.group
            },
            topic_options: {
                'auto.offset.reset': opConfig.offset_reset
            },
            rdkafka_options: {
                // We want to explicitly manage offset commits.
                'enable.auto.commit': false,
                'enable.auto.offset.store': false,
                'queued.min.messages': 2 * opConfig.size
            }
        }).client;
    }

    const consumer = createConsumer();

    return new Promise(((resolve) => {
console.log("Setting false initial")
        let readyToProcess = false;

        let rollbackOffsets = {};
        let pendingOffsets = {};
        let startingOffsets = {};
        let endingOffsets = {};

        // This sets up the stream that we're going to send all
        // data down.
        // const stream = new Rx.Subject();
        const stream = H(); // Empty Highland Stream

        // We need to add a checkpoint marker to the stream every opConfig.size
        // records. This will trigger downstream operators to commit state as
        // the checkpoint flows through.
        // TODO: Commits at the reader level are less clear.
        stream
            .observe()
            .filter(record => record instanceof StreamEntity)
            .batch(opConfig.size)
            //.windowCount(opConfig.size)
            //.subscribe(() => {
            .each(() => {
                pendingOffsets = endingOffsets;
                rollbackOffsets = startingOffsets;
                startingOffsets = {};
                endingOffsets = {};
console.log("Emitting a checkpoint.")
                stream.write(new CheckpointEntity(
                //stream.next(new CheckpointEntity(
                    /* TODO: need the slice_id here */
                ));
            });

        const kafkaError = Rx.Observable.fromEvent(consumer, 'error');
        let kafkaErrorSubscription;

        Rx.Observable
            .fromEvent(events, 'slice:success')
            .subscribe(() => {
                readyToProcess = false;

                try {
                    // Ideally we'd use commitSync here but it seems to throw
                    // an exception everytime it's called.
                    _.forOwn(pendingOffsets, (offset, partition) => {
                        consumer.commitSync({
                            partition: parseInt(partition, 10),
                            offset,
                            topic: opConfig.topic
                        });
                    });
                } catch (err) {
                    // If this is the first slice and the slice is Empty
                    // there may be no offsets stored which is not really
                    // an error.
                    if (err.code !== KAFKA_NO_OFFSET_STORED) {
                        jobLogger.error(`Kafka reader error after slice resolution ${err}`);
                    }
                }
            });

        const sliceFinalize = Rx.Observable
            .fromEvent(events, 'slice:finalize');

        sliceFinalize.subscribe(() => {
            readyToProcess = true;
            kafkaErrorSubscription.unsubscribe();
        });

        Rx.Observable
            .fromEvent(events, 'slice:retry')
            .subscribe(() => {
                readyToProcess = false;

                let count = _.keys(rollbackOffsets).length;
                if (count === 0) {
                    readyToProcess = true;
                }

                _.forOwn(rollbackOffsets, (offset, partition) => {
                    consumer.seek({
                        partition: parseInt(partition, 10),
                        offset,
                        topic: opConfig.topic
                    }, 1000, (err) => {
                        if (err) {
                            jobLogger.error(err);
                        }

                        count -= 1;
                        if (count === 0) {
                            readyToProcess = true;
                        }
                    });
                });
            });

        Rx.Observable
            .fromEvent(events, 'worker:shutdown')
            // Defer shutdown until final slice has finished.
            .sample(sliceFinalize)
            .subscribe(() => {
                //stream.end();
                stream.complete();
                readyToProcess = false;
                consumer.disconnect();
            });

        function initializeConsumer() {
            consumer.on('ready', () => {
                jobLogger.info('Consumer ready');
                consumer.subscribe([opConfig.topic]);

                // for debug logs.
                consumer.on('event.log', (event) => {
                    jobLogger.info(event);
                });

                readyToProcess = true;

                resolve(processSlice);
            });
        }

        initializeConsumer();
        let count = 0;

        function processSlice(data, logger) {
            return new Promise(((resolveSlice, reject) => {
                const consuming = setInterval(consume, 1);

                resolveSlice(stream);

                kafkaErrorSubscription = kafkaError.subscribe((err) => {
                    clearInterval(consuming);

                    logger.error(err);
                    reject(err);
                });

                function consume() {
                    // If we're blocking we don't want to complete or read
                    // data until unblocked.
                    if (!readyToProcess) return;

                    // We only want one consume call active at any given time
                    readyToProcess = false;

                    // Our goal is to get up to opConfig.batch messages but
                    // we may get less on each call.
                    consumer.consume(opConfig.batch_size, (err, messages) => {
                        if (err) {
                            // logger.error(err);
                            reject(err);
                            return;
                        }

                        messages.forEach((message) => {
                            // We want to track the first offset we receive so
                            // we can rewind if there is an error.
                            if (!startingOffsets[message.partition]) {
                                startingOffsets[message.partition] = message.offset;
                            }

                            // We record the last offset we see for each
                            // partition so that if the slice is successfull
                            // they can be committed.
                            endingOffsets[message.partition] = message.offset + 1;

                            let record = message.value;
                            if (opConfig.output_format === 'json') {
                                try {
                                    record = JSON.parse(message.value);
                                } catch (e) {
                                    logger.error('Invalid record ', e);
                                    // TODO: shunt off invalid records to dead letter office
                                }
                            }

                            count += 1;
                            if (count % 1000 === 0) {
                                console.log('Have read ', count, 'results');
                            }

                            /* stream.next(new StreamEntity(
                                record,
                                message.key,
                                new Date(message.timestamp)
                            )); */
                            stream.write(new StreamEntity(
                                record,
                                message.key,
                                new Date(message.timestamp)
                            ));
                        });

                        readyToProcess = true;
                    });
                }

                // Kick off initial processing.
                consume();
            }));
        }
    }));
}

function slicerQueueLength() {
    // Queue is not really needed so we just want the smallest queue size available.
    return 'QUEUE_MINIMUM_SIZE';
}

function newSlicer() {
    // The slicer actually has no work to do here.
    return Promise.resolve([() => new Promise((resolve) => {
        resolve(1);
    })]);
}

function schema() {
    return {
        topic: {
            doc: 'Name of the Kafka topic to process',
            default: '',
            format: 'required_String'
        },
        group: {
            doc: 'Name of the Kafka consumer group',
            default: '',
            format: 'required_String'
        },
        offset_reset: {
            doc: 'How offset resets should be handled when there are no valid offsets for the consumer group.',
            default: 'smallest',
            format: ['smallest', 'earliest', 'beginning', 'largest', 'latest', 'error']
        },
        size: {
            doc: 'How many records should be read before each slice checkpoint.',
            default: 10000,
            format: Number
        },
        batch_size: {
            doc: 'How many records to request for each batch',
            default: 1000,
            format: Number
        },
        connection: {
            doc: 'The Kafka consumer connection to use.',
            default: '',
            format: 'required_String'
        },
        rollback_on_failure: {
            doc: 'Controls whether the consumer state is rolled back on failure. This will protect against data loss, however this can have an unintended side effect of blocking the job from moving if failures are minor and persistent. NOTE: This currently defaults to `false` due to the side effects of the behavior, at some point in the future it is expected this will default to `true`.',
            default: false,
            format: Boolean
        },
        output_format: {
            doc: 'What format to return data in. Options: `json` or `raw`',
            default: 'json',
            format: ['json', 'raw']
        }
    };
}

module.exports = {
    newReader,
    newSlicer,
    schema,
    slicerQueueLength
};
