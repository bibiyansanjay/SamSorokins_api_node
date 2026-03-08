import { Router } from "express";
import tus from "./tus";

const router = Router();

router.use("/tus", tus);

export default router;
