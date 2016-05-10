export default {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "sensorId": {
            "type": "string"
        },
        "measurements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "dates": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "format": "date-time"
                        }
                    },
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
                            "type": "number"
                        }
                    },
                    "unitOfMeasurement": {
                        "type": "string"
                    }
                },
                "required": [
                    "date",
                    "type",
                    "source",
                    "values",
                    "unitOfMeasurement"
                ]
            }
        }
    },
    "required": [
        "sensorId",
        "measurements"
    ]
};
