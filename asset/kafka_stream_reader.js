'use strict';

const Promise = require('bluebird');
const _ = require('lodash');

const StreamEntity = require('./StreamEntity');
const StreamBatch = require('./stream-batch');

const KAFKA_NO_OFFSET_STORED = -168;

function newReader(context, opConfig) {
    const events = context.foundation.getEventEmitter();
    const jobLogger = context.logger;
    let rollbackOffsets = {};
    let pendingOffsets = {};
    let startingOffsets = {};
    let endingOffsets = {};
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
            'queued.min.messages': 2 * opConfig.size
        }
    }).client;

    const streamBatch = new StreamBatch(consumer, () => {
        jobLogger.info('finished batch');
    });

    events.on('worker:shutdown', () => {
        streamBatch.stop();
        consumer.unsubscribe();
        consumer.disconnect(() => {
            jobLogger.info('consumer disconnected');
        });
    });
    consumer.on('event.error', (err) => {
        jobLogger.error(err);
    });
    consumer.on('event.log', (event) => {
        jobLogger.info(event);
    });
    events.on('slice:retry', () => {
        if (_.isEmpty(rollbackOffsets)) {
            return;
        }
        streamBatch.stop();
        _.forOwn(rollbackOffsets, (offset, partition) => {
            consumer.seek({
                partition: parseInt(partition, 10),
                offset,
                topic: opConfig.topic
            }, 1000, (err) => {
                if (err) {
                    jobLogger.error(err);
                    return;
                }
                jobLogger.debug('consumer seek', { partition, offset });
            });
        });
    });
    events.on('slice:success', () => {
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
        pendingOffsets = endingOffsets;
        rollbackOffsets = startingOffsets;
        startingOffsets = {};
        endingOffsets = {};
    });
    const connectToConsumer = () => new Promise((resolve, reject) => {
        if (consumer.isConnected()) {
            resolve();
            return;
        }
        consumer.connect({}, (err) => {
            if (err) {
                reject(err);
                return;
            }
            resolve();
        });
    });
    return (data, sliceLogger) => connectToConsumer().then(() => {
        sliceLogger.info('starting new batch of ', opConfig.size);
        const batch = streamBatch.takeNext(opConfig.size, (message) => {
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
                    sliceLogger.error('Invalid record ', e);
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
        });
        return Promise.resolve(batch);
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
