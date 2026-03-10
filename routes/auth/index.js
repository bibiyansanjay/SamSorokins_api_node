import { Router } from "express";
import post from "./post";
import verifyOtp from "./verifyOtp";
import resendOtp from "./resendOtp";

/**
 * @namespace auth
 * @memberof module:Routes
 * @description Defines all auth routes.
 */

const router = Router();

router.post("/", post);
router.post("/verifyOtp", verifyOtp);
router.post("/resendOtp", resendOtp);
export default router;
