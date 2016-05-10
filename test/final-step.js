import {delay, resolve} from "bluebird";
import chai, {expect} from "chai";
import chaiAsPromised from "chai-as-promised";
import R from "ramda";
import sinon from "sinon";
import sinonChai from "sinon-chai";

chai.use(chaiAsPromised);
chai.use(sinonChai);

import finalStep from "final-step";

describe("convert", () => {

    const convert = finalStep.__get__("convert");

    var clock;
    const v4 = R.always("id");

    before(() => {
        finalStep.__Rewire__("v4", v4);
        clock = sinon.useFakeTimers();
    });
    after(() => {
        finalStep.__ResetDependency__("v4");
        clock.restore();
    });

    it("returns the disgregated array of readings", () => {
        const startingTime = new Date("2015-01-01").getTime();
        const delta = 5 * 60 * 1000;
        const getOffsetDate = (offset) => {
            return new Date(startingTime + (offset * delta));
        };
        const body = {
            sensorId: "sensorId",
            measurements: [
                {
                    type: "activeEnergy",
                    source: "forecast",
                    values: [1, 5, 8, 1000],
                    dates: [getOffsetDate(0), getOffsetDate(3), getOffsetDate(5), getOffsetDate(7)],
                    unitOfMeasurement: "kWh"
                },
                {
                    type: "maxPower",
                    source: "forecast",
                    values: [6, 5],
                    dates: [getOffsetDate(3), getOffsetDate(4)],
                    unitOfMeasurement: "kW"
                }
            ]
        };
        const readings = convert(body);
        // readings.forEach(reading => {
        //     console.log(reading.data.element);
        // });
        expect(readings).to.deep.equal([
            {
                id: "id",
                timestamp: new Date().toISOString(),
                type: "element inserted in collection readings",
                data: {
                    id: "id",
                    element: {
                        sensorId: "sensorId",
                        date: new Date(new Date("2015-01-01").getTime() + 0).toISOString(),
                        measurements: [{
                            type: "activeEnergy",
                            source: "forecast",
                            value: 1,
                            unitOfMeasurement: "kWh"
                        }]
                    }
                },
                source: {
                    kinesisPartitionKey: "sensorId"
                }
            },
            {
                id: "id",
                timestamp: new Date().toISOString(),
                type: "element inserted in collection readings",
                data: {
                    id: "id",
                    element: {
                        sensorId: "sensorId",
                        date: new Date(new Date("2015-01-01").getTime() + (3 * 5 * 60 * 1000)).toISOString(),
                        measurements: [
                            {
                                type: "activeEnergy",
                                source: "forecast",
                                value: 5,
                                unitOfMeasurement: "kWh"
                            },
                            {
                                type: "maxPower",
                                source: "forecast",
                                value: 6,
                                unitOfMeasurement: "kW"
                            }
                        ]
                    }
                },
                source: {
                    kinesisPartitionKey: "sensorId"
                }
            },
            {
                id: "id",
                timestamp: new Date().toISOString(),
                type: "element inserted in collection readings",
                data: {
                    id: "id",
                    element: {
                        sensorId: "sensorId",
                        date: new Date(new Date("2015-01-01").getTime() + (4 * 5 * 60 * 1000)).toISOString(),
                        measurements: [{
                            type: "maxPower",
                            source: "forecast",
                            value: 5,
                            unitOfMeasurement: "kW"
                        }]
                    }
                },
                source: {
                    kinesisPartitionKey: "sensorId"
                }
            },
            {
                id: "id",
                timestamp: new Date().toISOString(),
                type: "element inserted in collection readings",
                data: {
                    id: "id",
                    element: {
                        sensorId: "sensorId",
                        date: new Date(new Date("2015-01-01").getTime() + (5 * 5 * 60 * 1000)).toISOString(),
                        measurements: [{
                            type: "activeEnergy",
                            source: "forecast",
                            value: 8,
                            unitOfMeasurement: "kWh"
                        }]
                    }
                },
                source: {
                    kinesisPartitionKey: "sensorId"
                }
            },
            {
                id: "id",
                timestamp: new Date().toISOString(),
                type: "element inserted in collection readings",
                data: {
                    id: "id",
                    element: {
                        sensorId: "sensorId",
                        date: new Date(new Date("2015-01-01").getTime() + (7 * 5 * 60 * 1000)).toISOString(),
                        measurements: [{
                            type: "activeEnergy",
                            source: "forecast",
                            value: 1000,
                            unitOfMeasurement: "kWh"
                        }]
                    }
                },
                source: {
                    kinesisPartitionKey: "sensorId"
                }
            }
        ]);
    });

});

describe("putRecords", () => {

    const putRecords = finalStep.__get__("putRecords");

    const kinesis = {};

    before(() => {
        finalStep.__Rewire__("kinesis", kinesis);
    });
    after(() => {
        finalStep.__ResetDependency__("kinesis");
    });

    it("sequentially puts batches of 250 records into kinesis - sequentiality", () => {
        const invocations = [];
        kinesis.putRecords = sinon.spy(() => {
            invocations.push(Date.now());
            return delay(100);
        });
        const events = R.range(0, 1500).map(idx => ({
            id: idx,
            data: {
                element: {
                    sensorId: idx
                }
            }
        }));
        return putRecords(events).then(() => {
            invocations
                .map((date, idx) => (
                    idx === 0 ? 0 : date - invocations[idx - 1]
                ))
                .slice(1)
                .forEach(delta => {
                    expect(delta).to.be.closeTo(100, 20);
                });
        });
    });

    it("sequentially puts batches of 250 records into kinesis - batch size", () => {
        kinesis.putRecords = sinon.stub().returns(resolve(null));
        const events = R.range(0, 1500).map(idx => ({
            id: idx,
            data: {
                element: {
                    sensorId: idx
                }
            }
        }));
        return putRecords(events).then(() => {
            expect(kinesis.putRecords).to.have.callCount(6);
            R.range(0, 6)
                .map(idx => kinesis.putRecords.getCall(idx))
                .forEach(call => {
                    expect(call.args[0].Records.length).to.equal(250);
                });
        });
    });

    it("assigns each record a PartitionKey matching the event sensorId", () => {
        kinesis.putRecords = sinon.stub().returns(resolve(null));
        const events = R.range(0, 1500).map(idx => ({
            id: idx,
            data: {
                element: {
                    sensorId: idx
                }
            }
        }));
        return putRecords(events).then(() => {
            expect(kinesis.putRecords).to.have.callCount(6);
            const records = R.pipe(
                R.map(idx => kinesis.putRecords.getCall(idx)),
                R.map(call => call.args[0].Records),
                R.flatten
            )(R.range(0, 6));
            expect(records).to.have.length(1500);
            records.forEach(record => {
                const event = JSON.parse(record.Data);
                expect(record.PartitionKey).to.equal(event.data.element.sensorId);
            });
        });
    });

});
