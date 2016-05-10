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

function getMeasurementsAt (measurements, dateTime) {
    return R.pipe(
        R.map(m => ({
            type: m.type,
            source: m.source,
            value: m.values[R.indexOf(dateTime, m.dates)],
            unitOfMeasurement: m.unitOfMeasurement
        })),
        R.filter(m => !R.isNil(m.value))
    )(measurements);
}
function convert (body) {
    const allDates = R.uniq(
        R.flatten(
            body.measurements.map(m => m.dates)
        )
    ).sort();

    return R.pipe(
        R.map(dateTime => ({
            sensorId: body.sensorId,
            date: dateTime.toISOString(),
            measurements: getMeasurementsAt(body.measurements, dateTime)
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
    )(allDates);
}

export default function finalStep ({body}) {
    return BPromise.resolve(body)
        .then(convert)
        .then(putRecords)
        .thenReturn(null);
}
