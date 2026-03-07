import { Router } from "express";
import put from "./put";
const router = Router();

router.put("/", put);

export default router;
