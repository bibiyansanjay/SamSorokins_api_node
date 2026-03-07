import { Router } from "express";
import addPrice from "./addPrice";
// import put from "./put";
const router = Router();

// router.put("/", put);
router.post("/addPrice", addPrice);
export default router;
