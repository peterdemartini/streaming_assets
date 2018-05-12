'use strict'

const Promise = require('bluebird')
const _ = require('lodash')
const Kafka = require('node-rdkafka')

const H = require('highland')

const StreamEntity = require('./StreamEntity')

const KAFKA_NO_OFFSET_STORED = -168

function newReader (context, opConfig) {
  const events = context.foundation.getEventEmitter()
  const jobLogger = context.logger

  const consumerStream = Kafka.KafkaConsumer.createReadStream({
    'metadata.broker.list': 'localhost:9092',
    'group.id': opConfig.group,
    'socket.keepalive.enable': true,
    'enable.auto.commit': false,
    'enable.auto.offset.store': false,
    'queued.min.messages': 2 * opConfig.size
  },
  {
    'auto.offset.reset': opConfig.offset_reset
  },
  {
    topics: [opConfig.topic]
  })

  const stream = H(consumerStream)
  const consumer = consumerStream.consumer

  let rollbackOffsets = {}
  let pendingOffsets = {}
  let startingOffsets = {}
  let endingOffsets = {}

  events.on('slice:success', () => {
    try {
      // Ideally we'd use commitSync here but it seems to throw
      // an exception everytime it's called.
      _.forOwn(pendingOffsets, (offset, partition) => {
        consumer.commitSync({
          partition: parseInt(partition, 10),
          offset,
          topic: opConfig.topic
        })
      })
    } catch (err) {
      // If this is the first slice and the slice is Empty
      // there may be no offsets stored which is not really
      // an error.
      if (err.code !== KAFKA_NO_OFFSET_STORED) {
        jobLogger.error(`Kafka reader error after slice resolution ${err}`)
      }
    }
  })
  events.on('slice:retry', () => {
    if (_.isEmpty(rollbackOffsets)) {
      return
    }
    stream.pause()

    Promise.each(rollbackOffsets, (offset, partition) => {
      return new Promise((resolve, reject) => {
        consumer.seek({
          partition: parseInt(partition, 10),
          offset,
          topic: opConfig.topic
        }, 1000, (err) => {
          if (err) {
            jobLogger.error(err)
            return
          }
          resolve()
        })
      })
    }).then(() => {
      stream.resume()
    })
  })
  events.on('worker:shutdown', () => {
    stream.end()
  })
  consumer.on('event.log', (event) => {
    jobLogger.info(event)
  })
  consumerStream.on('error', (err) => {
    jobLogger.error(err)
  })

  stream
    .observe()
    .each((message) => {
      // We want to track the first offset we receive so
      // we can rewind if there is an error.
      if (!startingOffsets[message.partition]) {
        startingOffsets[message.partition] = message.offset
      }

      // We record the last offset we see for each
      // partition so that if the slice is successfull
      // they can be committed.
      endingOffsets[message.partition] = message.offset + 1
    })
    .batch(opConfig.size)
    .each(() => {
      pendingOffsets = endingOffsets
      rollbackOffsets = startingOffsets
      startingOffsets = {}
      endingOffsets = {}
    })
  stream.map((message) => {
    let record = message.value
    if (opConfig.output_format === 'json') {
      try {
        record = JSON.parse(message.value)
      } catch (e) {
        jobLogger.error('Invalid record ', e)
        // TODO: shunt off invalid records to dead letter office
      }
    }
    return new StreamEntity(
      record,
      message.key,
      new Date(message.timestamp)
    )
  })
  return stream
}

function slicerQueueLength () {
  // Queue is not really needed so we just want the smallest queue size available.
  return 'QUEUE_MINIMUM_SIZE'
}

function newSlicer () {
  // The slicer actually has no work to do here.
  return Promise.resolve([() => new Promise((resolve) => {
    resolve(1)
  })])
}

function schema () {
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
  }
}

module.exports = {
  newReader,
  newSlicer,
  schema,
  slicerQueueLength
}
