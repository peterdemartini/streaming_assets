'use strict';

const H = require('highland');

function StreamBatch(client, onFinish) {
    let batchStream;
    let stopped = false;
    this.takeNext = (count, fn) => {
        batchStream = H((push) => {
            client.once('unsubscribed', () => {
                push(new Error('client unsubscribed'));
                push(null, H.nil);
                stopped = true;
            });
            const pullNext = (i) => {
                if (i >= count) {
                    push(null, H.nil);
                    onFinish();
                    return;
                }
                client.consume((err, message) => {
                    if (err) {
                        push(err);
                        // wait before we retry
                        // setTimeout(() => {
                        //     pullNext(i);
                        // }, Math.random() * 1000).unref();
                        return;
                    }
                    if (stopped) {
                        setTimeout(pullNext, 10, i);
                        return;
                    }
                    push(null, fn(message));
                    pullNext(i - 1);
                });
            };
            pullNext(count);
        });
        return batchStream;
    };
    this.stop = () => {
        stopped = true;
    };
    this.continue = () => {
        stopped = false;
    };
}

module.exports = StreamBatch;
