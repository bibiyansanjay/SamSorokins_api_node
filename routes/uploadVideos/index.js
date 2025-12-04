import { Router } from "express";
import tus from "./tus";

const router = Router();

router.post("/tus", tus);

export default router;
