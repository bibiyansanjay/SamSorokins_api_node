import { Router } from "express";
import post from "./post";
import verifyOtp from "./verifyOtp";
import resendOTP from "./resendOTP";

const router = Router();

router.post("/", post);
router.post("/verifyOtp", verifyOtp);
router.post("/resendOtp", resendOTP);
export default router;
