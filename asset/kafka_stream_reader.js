'use strict';

const Promise = require('bluebird');
const _ = require('lodash');
const { Stream, StreamEntity } = require('teraslice_stream');
const StreamBatch = require('./stream-batch');

const KAFKA_NO_OFFSET_STORED = -168;

function newReader(context, opConfig) {
    const events = context.foundation.getEventEmitter();
    const jobLogger = context.logger;
    let rollbackOffsets = {};
    const startingOffsets = {};
    const endingOffsets = {};
    const batchSize = opConfig.size;
    const consumer = context.foundation.getConnection({
        type: 'kafka',
        endpoint: opConfig.connection,
        autoconnect: false,
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
            'queued.min.messages': 2 * batchSize
        }
    }).client;
    const commit = Promise.promisify(consumer.commit, { context: consumer });
    const seek = Promise.promisify(consumer.seek, { context: consumer });

    const streamBatch = new StreamBatch(consumer, () => {
        jobLogger.info('kafka_stream_reader finished batch');
    });

    let committing = false;
    let retrying = false;
    let shuttingDown = false;
    const waitForCommit = (callback) => {
        const check = () => {
            if (committing) {
                _.delay(check, 100);
                return;
            }
            callback();
        };
        check();
    };
    const waitForRetry = (callback) => {
        const check = () => {
            if (retrying) {
                _.delay(check, 100);
                return;
            }
            callback();
        };
        check();
    };
    events.on('worker:shutdown', () => {
        shuttingDown = true;
        waitForCommit(() => {
            streamBatch.end();
            consumer.unsubscribe();
            consumer.disconnect(() => {
                jobLogger.info('kafka_stream_reader consumer disconnected');
            });
        });
    });
    consumer.on('event.error', (err) => {
        if (shuttingDown) return;
        jobLogger.error(err);
    });
    consumer.on('event.log', (event) => {
        if (shuttingDown) return;
        jobLogger.info(event);
    });
    events.on('slice:retry', () => {
        if (shuttingDown) return;
        if (_.isEmpty(rollbackOffsets)) {
            return;
        }
        retrying = true;
        streamBatch.end();
        const rollbacks = _.map(rollbackOffsets, (offset, partition) => {
            jobLogger.debug('consumer seek', { partition, offset });
            return seek({
                partition: parseInt(partition, 10),
                offset,
                topic: opConfig.topic
            }, 1000);
        });
        Promise.all(rollbacks).then(() => {
            retrying = false;
        }).catch((err) => {
            retrying = false;
            if (err) {
                jobLogger.error(err);
            }
        });
    });
    events.on('slice:success', () => {
        committing = true;
        const commits = _.map(endingOffsets, (offset, partition) => commit({
            partition: parseInt(partition, 10),
            offset,
            topic: opConfig.topic
        }));
        Promise.all(commits).catch((err) => {
            // If this is the first slice and the slice is Empty
            // there may be no offsets stored which is not really
            // an error.
            if (err.code !== KAFKA_NO_OFFSET_STORED) {
                jobLogger.error(`Kafka reader error after slice resolution ${err}`);
                return Promise.resolve();
            }
            committing = false;
            return Promise.reject(err);
        }).then(() => {
            committing = false;
            // We keep track of where we start reading for each slice.
            // If there is an error we'll rewind the consumer and read
            // the slice again.
            rollbackOffsets = startingOffsets;
        });
    });
    const connectToConsumer = () => new Promise((resolve, reject) => {
        if (shuttingDown) {
            reject(new Error('Processor shutting down'));
            return;
        }
        if (consumer.isConnected()) {
            waitForRetry(() => {
                resolve();
            });
            return;
        }
        consumer.connect();
        consumer.on('ready', () => {
            consumer.setDefaultConsumeTimeout(100);
            consumer.subscribe([opConfig.topic]);
            resolve();
        });
    });
    return (data, sliceLogger) => connectToConsumer().then(() => {
        sliceLogger.info(`kafka_stream_reader starting new batch of ${batchSize}`);
        const handleMessage = (message) => {
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
                    record = JSON.parse(record);
                } catch (e) {
                    sliceLogger.error('Kafka reader got an invalid record ', e);
                    // TODO: shunt off invalid records to dead letter office
                }
            }
            return new StreamEntity(
                record,
                {
                    key: message.key,
                    processTime: new Date(),
                    ingestTime: new Date(message.timestamp),
                }
            );
        };
        const batch = streamBatch.takeNext(batchSize, handleMessage, sliceLogger);
        return Promise.resolve(new Stream(batch));
    });
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
