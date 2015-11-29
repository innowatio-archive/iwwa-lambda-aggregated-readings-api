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

    it("returns the disgregated array of readings", () => {
        const body = {
            sensorId: "sensorId",
            date: new Date("2015-01-01").toISOString(),
            timeStep: (5 * 60 * 1000),
            measurements: [
                {
                    type: "activeEnergy",
                    source: "forecast",
                    values: [1, null, null, 5, null, 8, null, 1000, null],
                    unitOfMeasurement: "kWh"
                },
                {
                    type: "maxPower",
                    source: "forecast",
                    values: [null, null, null, 6, 5],
                    unitOfMeasurement: "kW"
                }
            ]
        };
        const readings = convert(body);
        expect(readings).to.deep.equal([
            {
                sensorId: "sensorId",
                date: new Date(new Date("2015-01-01").getTime() + 0).toISOString(),
                measurements: [{
                    type: "activeEnergy",
                    source: "forecast",
                    value: 1,
                    unitOfMeasurement: "kWh"
                }]
            },
            {
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
            },
            {
                sensorId: "sensorId",
                date: new Date(new Date("2015-01-01").getTime() + (4 * 5 * 60 * 1000)).toISOString(),
                measurements: [{
                    type: "maxPower",
                    source: "forecast",
                    value: 5,
                    unitOfMeasurement: "kW"
                }]
            },
            {
                sensorId: "sensorId",
                date: new Date(new Date("2015-01-01").getTime() + (5 * 5 * 60 * 1000)).toISOString(),
                measurements: [{
                    type: "activeEnergy",
                    source: "forecast",
                    value: 8,
                    unitOfMeasurement: "kWh"
                }]
            },
            {
                sensorId: "sensorId",
                date: new Date(new Date("2015-01-01").getTime() + (7 * 5 * 60 * 1000)).toISOString(),
                measurements: [{
                    type: "activeEnergy",
                    source: "forecast",
                    value: 1000,
                    unitOfMeasurement: "kWh"
                }]
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
        const readings = R.range(0, 1500).map(idx => ({idx}));
        return putRecords(readings).then(() => {
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
        const readings = R.range(0, 1500).map(idx => ({idx}));
        return putRecords(readings).then(() => {
            expect(kinesis.putRecords).to.have.callCount(6);
            R.range(0, 6)
                .map(idx => kinesis.putRecords.getCall(idx))
                .forEach(call => {
                    expect(call.args[0].Records.length).to.equal(250);
                });
        });
    });

});
