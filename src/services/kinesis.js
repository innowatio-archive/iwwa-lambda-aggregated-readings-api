import {Kinesis} from "aws-sdk";
import {promisify} from "bluebird";

const kinesis = new Kinesis({
    apiVersion: "2013-12-02"
});

export const putRecords = promisify(kinesis.putRecords, {context: kinesis});
