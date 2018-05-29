'use strict';

const Promise = require('bluebird');
const _ = require('lodash');

function newProcessor(context, opConfig) {
    const jobLogger = context.logger;
    const bufferSize = 5 * opConfig.size;
    let processed = 0;
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
        }
    }).client;

    function flush() {
        return new Promise((resolve, reject) => {
            producer.flush(60000, (err) => {
                if (err != null) {
                    jobLogger.error(err);
                    reject(err);
                    return;
                }
                processed = 0;
                resolve();
            });
        });
    }
    producer.on('event.error', (err) => {
        jobLogger.error(err);
    });
    function connect() {
        return new Promise((resolve) => {
            if (producer.isConnected()) {
                resolve();
                return;
            }
            producer.connect();
            producer.once('ready', () => {
                resolve();
            });
        });
    }
    connect();
    function getTimestamp(record) {
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
    }
    function produce(record) {
        const key = _.get(record.data, opConfig.id_field, record.key);
        const timestamp = getTimestamp(record);
        const err = producer.produce(
            opConfig.topic,
            null,
            record.toBuffer(),
            key,
            timestamp
        );
        if (err != null && err !== true) {
            return err;
        }
        return null;
    }
    return function processor(stream, sliceLogger) {
        sliceLogger.info('kafka_stream_sender starting batch');
        function handleRecord(record) {
            const produceErr = produce(record);
            if (produceErr != null && produceErr !== true) {
                sliceLogger.warn(`kafka_stream_sender producer queue is full ${processed}/${bufferSize}`, produceErr);
                processed = bufferSize;
            } else {
                processed += 1;
            }
            if (processed >= opConfig.size) {
                return flush();
            }
            return Promise.resolve();
        }
        function handleStream() {
            if (opConfig.continue_stream) {
                return stream.each(handleRecord);
            }
            return stream.each(handleRecord).done();
        }

        return connect().then(handleStream);
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
