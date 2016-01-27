import BPromise from "bluebird";
import {v4} from "node-uuid";
import R from "ramda";

import * as config from "./services/config";
import * as kinesis from "./services/kinesis";
import log from "./services/logger";

function _putRecords (events) {
    const records = events.map(event => ({
        Data: JSON.stringify(event),
        PartitionKey: event.data.element.sensorId
    }));
    log.debug({records}, "Putting Kinesis records");
    return kinesis.putRecords({
        Records: records,
        StreamName: config.KINESIS_STREAM_NAME
    });
}
function putRecords (events) {
    const batches = R.splitEvery(250, events);
    return BPromise.map(batches, _putRecords, {concurrency: 1});
}

function getMeasurementsAt (measurements, offset) {
    return R.pipe(
        R.map(m => ({
            type: m.type,
            source: m.source,
            value: m.values[offset],
            unitOfMeasurement: m.unitOfMeasurement
        })),
        R.filter(m => !R.isNil(m.value))
    )(measurements);
}
function getDateAt (startDate, timeStep, offset) {
    const ms = startDate + (offset * timeStep);
    return new Date(ms).toISOString();
}
function convert (body) {
    const startDate = new Date(body.date).getTime();
    const lengths = body.measurements.map(m => m.values.length);
    const maxLength = Math.max(...lengths);
    const maxRange = R.range(0, maxLength);
    return R.pipe(
        R.map(offset => ({
            sensorId: body.sensorId,
            date: getDateAt(startDate, body.timeStep, offset),
            measurements: getMeasurementsAt(body.measurements, offset)
        })),
        R.filter(reading => !R.isEmpty(reading.measurements)),
        R.map(reading => ({
            id: v4(),
            timestamp: new Date().toISOString(),
            type: "element inserted in collection readings",
            data: {
                id: v4(),
                element: reading
            },
            source: {
                kinesisPartitionKey: reading.sensorId
            }
        }))
    )(maxRange);
}

export default function finalStep ({body}) {
    return BPromise.resolve(body)
        .then(convert)
        .then(putRecords)
        .thenReturn(null);
}
