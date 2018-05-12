'use strict'

const Promise = require('bluebird')

function newProcessor (context, opConfig) {
  const _producerReady = false
  const foundationEvents = context.foundation.getEventEmitter()

  const bufferSize = 5 * opConfig.size

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
  }).client

  const producerReady = (callback) => {
    if (_producerReady) {
      callback()
      return
    }
    producer.once('ready', () => {
      callback()
    })
  }
  return function processor (stream) {
    return new Promise((resolve) => {
      foundationEvents.once('worker:shutdown', () => {
        stream.end()
      })
      stream.fork()
        .filter(record => record)
        .batchWithTimeOrCount(opConfig.wait, opConfig.size)
        .each(send)
        .done(resolve)

      function send (records) {
        producerReady(() => {
          records.forEach((record) => {
            producer.produce(
              opConfig.topic,
              // This is the partition. There may be use cases where
              // we'll need to control this.
              null,
              Buffer.from(JSON.stringify(record.data)),
              record.key, // key
              record.processTime // timestamp ... which one to use here?
            )
          })
        })
      }
    })
  }
}

function schema () {
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
  }
}

function checkpointAware () {
  return true
}

module.exports = {
  newProcessor,
  checkpointAware,
  schema
}
