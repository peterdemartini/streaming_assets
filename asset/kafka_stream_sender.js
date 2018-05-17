'use strict';

const Promise = require('bluebird');
const _ = require('lodash');
const H = require('highland');

function newProcessor(context, opConfig) {
    const jobLogger = context.logger;
    const events = context.foundation.getEventEmitter();
    const bufferSize = 5 * opConfig.size;
    let currentBufferSize = 0;
    const producer = context.foundation.getConnection({
        type: 'kafka',
        endpoint: opConfig.connection,
        options: {
            type: 'producer'
        },
        autoconnect: false,
        rdkafka_options: {
            'compression.codec': opConfig.compression,
            'queue.buffering.max.messages': bufferSize,
            'queue.buffering.max.ms': opConfig.wait,
            'batch.num.messages': opConfig.size,
            'topic.metadata.refresh.interval.ms': opConfig.metadata_refresh,
            'log.connection.close': false
        }
    }).client;
    const shouldFlush = () => {
        if (currentBufferSize > bufferSize) {
            return true;
        }
        // better to be save than sorry
        const upper = bufferSize + (opConfig.size / 2);
        const lower = bufferSize - (opConfig.size / 2);
        return _.inRange(currentBufferSize, lower, upper);
    };
    const flush = (callback) => {
        producer.flush(60000, (err) => {
            if (err != null) {
                jobLogger.error(err);
                callback(err);
                return;
            }
            currentBufferSize = 0;
            callback();
        });
    };
    producer.on('event.error', (err) => {
        jobLogger.error(err);
    });
    const connect = () => new Promise((resolve, reject) => {
        if (producer.isConnected()) {
            resolve();
            return;
        }
        producer.connect({}, (err) => {
            if (err) {
                reject(err);
                return;
            }
            resolve();
        });
    });

    const getTimestamp = (record) => {
        let date;
        const now = opConfig.timestamp_now;
        const field = opConfig.timestamp_field;
        if (field) {
            date = _.get(record.data, field);
        } else if (now) {
            date = new Date();
        }
        if (date == null) {
            date = record.processTime;
        }
        if (!_.isDate(date)) {
            date = new Date(date);
        }
        return date.getTime();
    };
    return function processor(stream, sliceLogger) {
        return connect().then(() => new Promise((resolve, reject) => {
            let shuttingDown = false;
            const shutdown = () => {
                events.removeListener('worker:shutdown', shutdown);
                shuttingDown = true;
                flush(() => {
                    resolve();
                });
            };
            const results = [];
            events.on('worker:shutdown', shutdown);
            sliceLogger.info('kafka_stream_sender starting batch');
            stream
                .stopOnError((err) => {
                    if (shuttingDown) {
                        sliceLogger.error('kafka_stream_sender stream error when shutting down', err);
                        return;
                    }
                    if (err) {
                        sliceLogger.error('kafka_stream_sender stream error', err);
                        reject(err);
                    }
                })
                .each((record) => {
                    results.push(record);
                    const key = _.get(record.data, opConfig.id_field, record.key);
                    const timestamp = getTimestamp(record);
                    producer
                        .produce(
                            opConfig.topic,
                            null,
                            record.toBuffer(),
                            key,
                            timestamp
                        );
                }).done(() => {
                    events.removeListener('worker:shutdown', shutdown);
                    if (shuttingDown) {
                        sliceLogger.info('kafka_stream_sender slice finished but waiting to producer to be flushed before shutting down');
                        return;
                    }
                    currentBufferSize += _.size(results);
                    sliceLogger.info('kafka_stream_sender finished batch', _.size(results));
                    if (shouldFlush()) {
                        flush(() => {
                            resolve(H(results));
                        });
                    } else {
                        resolve(H(results));
                    }
                });
        }));
    };
}

function schema() {
    return {
        topic: {
            doc: 'Name of the Kafka topic to send data to',
            default: '',
            format: 'required_String'
        },
        id_field: {
            doc: 'Field in the incoming record that contains keys',
            default: '',
            format: String
        },
        timestamp_field: {
            doc: 'Field in the incoming record that contains a timestamp to set on the record',
            default: '',
            format: String
        },
        timestamp_now: {
            doc: 'Set to true to have a timestamp generated as records are added to the topic',
            default: '',
            format: String
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
