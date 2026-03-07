import { Router } from "express";
// import put from "./put";
import userId from "./userId";
import extractParam from "../../middlewares/extractParam";
const router = Router();

// router.put("/", put);
router.use("/:userId", extractParam("userId"), userId);
export default router;
