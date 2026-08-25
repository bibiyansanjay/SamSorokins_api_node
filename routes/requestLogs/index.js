import { Router } from "express";
import get from "./get.js";

const router = Router();

router.get("/", get);

export default router;
