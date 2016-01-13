export default {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "sensorId": {
            "type": "string"
        },
        "date": {
            "type": "string",
            "format": "date-time"
        },
        "timeStep": {
            "type": "number"
        },
        "source": {
            "type": "string",
            "enum": ["forecast", "reading"]
        },
        "measurements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["activeEnergy", "reactiveEnergy", "maxPower", "temperature", "humidity", "illuminance", "co2"]
                    },
                    "source": {
                        "type": "string",
                        "enum": ["forecast", "reading"]
                    },
                    "values": {
                        "type": "array",
                        "items": {
                            "oneOf": [
                                {"type": "number"},
                                {"type": "null"}
                            ]
                        }
                    },
                    "unitOfMeasurement": {
                        "type": "string"
                    }
                },
                "required": [
                    "type",
                    "values",
                    "unitOfMeasurement"
                ]
            }
        }
    },
    "required": [
        "sensorId",
        "date",
        "timeStep",
        "measurements"
    ]
};
