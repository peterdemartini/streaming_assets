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
            'log.connection.close': false,
            dr_cb: true
        }
    }).client;

    producer.on('delivery-report', (err) => {
        if (err) {
            jobLogger.error('delivery report error', err);
            return;
        }
        currentBufferSize -= 1;
    });
    const shouldFlush = () => {
        if (currentBufferSize === 0) {
            return false;
        }
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
    const flushIfNeeded = (callback) => {
        if (shouldFlush()) {
            flush(callback);
            return;
        }
        callback();
    };
    producer.on('event.error', (err) => {
        jobLogger.error(err);
    });
    const connect = () => new Promise((resolve) => {
        if (producer.isConnected()) {
            resolve();
            return;
        }
        producer.connect();
        producer.once('ready', () => {
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
                    shuttingDown = false;
                });
            };
            const resultStream = H();
            events.on('worker:shutdown', shutdown);
            const finishBatch = (err) => {
                if (shuttingDown) {
                    sliceLogger.warn('kafka_stream_sender already shutting down', err);
                    return;
                }
                if (err) {
                    sliceLogger.error('kafka_stream_sender stream error', err);
                    reject(err);
                    if (opConfig.continue_stream) {
                        resultStream.destroy();
                    }
                    return;
                }
                events.removeListener('worker:shutdown', shutdown);
                flushIfNeeded(() => {
                    sliceLogger.info('kafka_stream_sender finished batch', { currentBufferSize });
                    if (opConfig.continue_stream) {
                        resultStream.end();
                        resolve(resultStream);
                    } else {
                        stream.end();
                        resolve();
                    }
                });
            };
            sliceLogger.info('kafka_stream_sender starting batch');
            stream.consume((err, record, push, next) => {
                if (err) {
                    finishBatch(err);
                } else if (record === H.nil) {
                    finishBatch();
                } else {
                    const produce = () => {
                        const key = _.get(record.data, opConfig.id_field, record.key);
                        const timestamp = getTimestamp(record);
                        const produceErr = producer
                            .produce(
                                opConfig.topic,
                                null,
                                record.toBuffer(),
                                key,
                                timestamp
                            );
                        if (produceErr != null && produceErr !== true) {
                            sliceLogger.warn('kafka_stream_sender producer queue is full', produceErr);
                            _.delay(produce, _.random(0, 100));
                            return;
                        }
                        currentBufferSize += 1;
                        if (opConfig.continue_stream) {
                            resultStream.write(record);
                        }
                        next();
                    };
                    produce();
                }
            }).resume();
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
        },
        continue_stream: {
            doc: 'Return the stream on the completion of the batch',
            default: false,
            format: Boolean
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
