/**
 * @Function Handle Files by submission id
 * @Description Handle Uploaded Files by submissions id
 */

import { Router } from "express";

import get from "./get";

const router = Router();

router.get("/", get);
// router.post("/", post);
// router.use("/deleted", deletedUser);
export default router;
