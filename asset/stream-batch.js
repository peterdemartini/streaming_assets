'use strict';

const H = require('highland');
const _ = require('lodash');

function StreamBatch(client, onFinish) {
    let batchStream;
    const finish = () => {
        if (batchStream == null) {
            return;
        }
        if (!batchStream.ended) {
            batchStream.end();
        }
        onFinish();
    };
    this.takeNext = (batchSize, fn, logger) => {
        if (!_.isNumber(batchSize)) {
            throw new Error('Incorrect batch size');
        }
        batchStream = H((push) => {
            const pullNext = (remaining) => {
                logger.debug(`stream_batch: pullNext (${remaining})`);
                if (batchStream.ended) {
                    logger.debug('stream_batch: batchStream ended');
                    return;
                }
                if (!remaining) {
                    logger.debug('stream_batch: batchStream done');
                    finish();
                    return;
                }
                if (batchSize !== remaining && batchStream.paused) {
                    logger.debug('stream_batch: batchStream paused, waiting until it is available');
                    _.delay(pullNext, Math.random() * 1000, remaining);
                    return;
                }
                logger.debug('stream_batch: consuming...');
                client.consume(remaining, (err, messages) => {
                    if (err) {
                        logger.error('stream_batch: got message with error', err);
                        push(err);
                        // wait before we retry
                        _.delay(pullNext, Math.random() * 1000, remaining);
                        return;
                    }
                    logger.debug(`stream_batch: got messages ${_.size(messages)}`);
                    _.forEach(messages, (message) => {
                        push(null, fn(message));
                    });
                    pullNext(remaining - _.size(messages));
                });
            };
            pullNext(batchSize);
        });
        return batchStream;
    };
    this.end = () => {
        finish();
    };
}

module.exports = StreamBatch;
