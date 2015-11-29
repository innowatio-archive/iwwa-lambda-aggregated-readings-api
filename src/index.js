import connect from "lambda-connect";
import validateBody from "lcm-validate-body";

import finalStep from "./final-step";
import schema from "./schema";
import log from "./services/logger";

export const handler = connect({log})
    .use(validateBody(schema))
    .use(finalStep);
