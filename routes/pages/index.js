import { Router } from "express";
import get from "./get";
import videos from "./videos";
import getSelectOptions from "./getSelectOptions";

const router = Router();
router.get("/", get);
router.get("/videos", videos);
router.get("/getSelectOptions", getSelectOptions);

export default router;
