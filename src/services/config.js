import dotenv from "dotenv";

dotenv.load();

export const KINESIS_STREAM_NAME = process.env.KINESIS_STREAM_NAME;
export const KINESIS_PARTITION_KEY = "aggretated-readings-api";
