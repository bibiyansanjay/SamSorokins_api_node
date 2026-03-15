import { Router } from "express";

import extractParam from "../../middlewares/extractParam";
import get from "./get";

/**
 * @namespace forgotPassword
 * @memberof module:Routes
 * @description Defines all forgotPassword routes.
 */

const router = Router();

router.get("/:submissionId", extractParam("submissionId"), get);

export default router;
