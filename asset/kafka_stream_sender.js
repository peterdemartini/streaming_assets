'use strict';

const Promise = require('bluebird');
const _ = require('lodash');
const H = require('highland');

function newProcessor(context, opConfig) {
    const jobLogger = context.logger;
    const events = context.foundation.getEventEmitter();
    const bufferSize = 5 * opConfig.size;
    let flushAfterBatch;

    const producerStream = context.foundation.getConnection({
        type: 'kafka',
        endpoint: opConfig.connection,
        options: {
            type: 'producer-stream'
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
    const producer = producerStream.producer;
    const flush = (callback) => {
        producer.flush(60000, (err) => {
            if (err != null) {
                jobLogger.error(err);
                callback(err);
            }
            callback();
        });
    };
    const newFlushAfterBatch = () => _.after(opConfig.size, () => {
        flush(_.noop);
        flushAfterBatch = newFlushAfterBatch();
    });
    flushAfterBatch = newFlushAfterBatch();
    events.on('worker:shutdown', () => {
        flush(() => {
            producerStream.close();
        });
    });
    producerStream.on('error', (err) => {
        jobLogger.error(err);
    });
    producer.on('event.error', (err) => {
        jobLogger.error(err);
    });

    return function processor(stream, sliceLogger) {
        return new Promise((resolve, reject) => {
            sliceLogger.info('starting batch');
            stream
                .consume((err, record, push, next) => {
                    if (err) {
                        // pass errors along the stream and consume next value
                        push(err);
                        next();
                        return;
                    } else if (record === H.nil) {
                        // pass nil (end event) along the stream
                        push(null, record);
                        return;
                    }
                    flushAfterBatch();
                    producerStream.write({
                        topic: opConfig.topic,
                        partition: null,
                        value: Buffer.from(JSON.stringify(record.data)),
                        key: record.key,
                        timestamp: record.processTime,
                    }, () => {
                        push(null, record);
                        next();
                    });
                }).stopOnError(reject).toArray((results) => {
                    sliceLogger.info('finished batch');
                    resolve(H(results));
                });
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
