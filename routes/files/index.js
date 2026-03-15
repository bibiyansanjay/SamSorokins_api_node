/**
 * @Function Handle Files
 * @Description Handle Uploaded Files
 */

import { Router } from "express";

import extractParam from "../../middlewares/extractParam";
import submissionId from "./submissionId";
import get from "./get";

const router = Router();

router.use(
  "/submissionId/:submissionId",
  extractParam("submissionId"),
  submissionId
);
router.get("/", get);
// router.post("/", post);
// router.use("/deleted", deletedUser);
export default router;
