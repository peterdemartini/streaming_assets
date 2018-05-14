'use strict';

const Promise = require('bluebird');

function newProcessor(context, opConfig) {
    const jobLogger = context.logger;
    const events = context.foundation.getEventEmitter();

    const bufferSize = 5 * opConfig.size;

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
    events.on('worker:shutdown', () => {
        producer.flush(60000, (err) => {
            producer.removeListener('event.error', err);
            if (err) {
                jobLogger.error(err);
                return;
            }
            producerStream.close();
        });
    });
    producerStream.on('error', (err) => {
        jobLogger.error(err);
    });
    producer.on('event.error', (err) => {
        jobLogger.error(err);
    });

    return function processor(stream) {
        return new Promise((resolve, reject) => {
            stream
                .stopOnError((err) => {
                    reject(err);
                })
                .map((record) => {
                    producerStream.write({
                        topic: opConfig.topic,
                        partition: null,
                        value: Buffer.from(JSON.stringify(record.data)),
                        key: record.key,
                        timestamp: record.processTime,
                    });
                    return record;
                }).toArray((result) => {
                    producer.flush(60000, (err) => {
                        producer.removeListener('event.error', err);
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(result);
                    });
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
