import { Router } from "express";
import post from "./post";
import verifyOtp from "./verifyOtp";

/**
 * @namespace auth
 * @memberof module:Routes
 * @description Defines all auth routes.
 */

const router = Router();

router.post("/", post);
router.post("/verifyOtp", verifyOtp);
export default router;
