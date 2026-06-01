/**
 * @Function Handle Files
 * @Description Handle submission Files
 */

import { Router } from "express";
import get from "./get";
import del from "./delete.js";
import sendReminder from "./sendReminder.js";
import extractParam from "../../middlewares/extractParam";

const router = Router();

router.get("/", get);
router.delete("/:submissionId", extractParam("submissionId"), del);
router.post(
  "/:submissionId/send-reminder",
  extractParam("submissionId"),
  sendReminder
);

export default router;
