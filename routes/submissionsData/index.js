/**
 * @Function Handle Files
 * @Description Handle submission Files
 */

import { Router } from "express";
import get from "./get";
import getOptions from "./getOptions.js";
import getDuplicates from "./getDuplicates.js";
import getResolvedDuplicates from "./getResolvedDuplicates.js";
import resolveDuplicates from "./resolveDuplicates.js";
import del from "./delete.js";
import sendReminder from "./sendReminder.js";
import extractParam from "../../middlewares/extractParam";

const router = Router();

router.get("/options", getOptions);
router.get("/duplicates", getDuplicates);
router.get("/resolved-duplicates", getResolvedDuplicates);
router.post("/resolve-duplicates", resolveDuplicates);
router.get("/", get);
router.delete("/:submissionId", extractParam("submissionId"), del);
router.post(
  "/:submissionId/send-reminder",
  extractParam("submissionId"),
  sendReminder
);

export default router;
