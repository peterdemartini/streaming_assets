'use strict';

/**
 * key - A unique key for the data entity
 * data - The data item, type of this field is open but most likely an object.
 * ingestTime - The time the data was ingested if available. This must be
 *      provided by the source of the data.
 * processTime - Time set by the reader at the point when the data is initially
 *      brought into the Teraslice pipeline.
 * eventTime - Time associated with actual event that is extracted from the
 *      data record.
 */
const StreamEntity = function StreamEntity(data, key, ingestTime, processTime, eventTime) {
    this.key = key;
    this.data = data;
    this.ingestTime = ingestTime;
    this.processTime = processTime;
    this.eventTime = eventTime;
};

/* StreamEntity.prototype.x = function x(value) {
}; */

module.exports = StreamEntity;
