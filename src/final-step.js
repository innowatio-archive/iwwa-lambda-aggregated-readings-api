import BPromise from "bluebird";
import R from "ramda";

import * as config from "./services/config";
import * as kinesis from "./services/kinesis";
import log from "./services/logger";

function _putRecords (arrayOfData) {
    const records = arrayOfData.map(data => ({
        Data: JSON.stringify(data),
        PartitionKey: config.KINESIS_PARTITION_KEY
    }));
    log.debug({records}, "Putting Kinesis records");
    return kinesis.putRecords({
        Records: records,
        StreamName: config.KINESIS_STREAM_NAME
    });
}
function putRecords (arrayOfData) {
    const batches = R.splitEvery(250, arrayOfData);
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
        R.filter(reading => !R.isEmpty(reading.measurements))
    )(maxRange);
}

export default function finalStep ({body}) {
    return BPromise.resolve(body)
        .then(convert)
        .then(putRecords)
        .thenReturn(null);
}
