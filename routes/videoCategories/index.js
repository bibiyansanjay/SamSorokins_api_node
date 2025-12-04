import { Router } from "express";
import get from "./get";
import getSelectOptions from "./getSelectOptions";

const router = Router();
router.get("/", get);
router.get("/getSelectOptions", getSelectOptions);

export default router;
