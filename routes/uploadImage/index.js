import { Router } from "express";
import userId from "./userId";

import extractParam from "../../middlewares/extractParam";

const router = Router();

router.use("/user/:userId", extractParam("userId"), userId);

export default router;
