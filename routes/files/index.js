/**
 * @Function Handle Files
 * @Description Handle Uploaded Files
 */

import { Router } from "express";

import extractParam from "../../middlewares/extractParam";
import submissionId from "./submissionId";
import get from "./get";
import filesWithFormData from "./filesWithFormData";

const router = Router();

router.use(
  "/submissionId/:submissionId",
  extractParam("submissionId"),
  submissionId
);
router.get(
  "/formData/:submissionId",
  extractParam("submissionId"),
  filesWithFormData
);
router.get("/", get);
// router.post("/", post);
// router.use("/deleted", deletedUser);
export default router;
