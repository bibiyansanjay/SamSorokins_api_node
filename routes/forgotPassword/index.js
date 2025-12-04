import { Router } from "express";
import post from "./post";
import resendOTP from "./resendOTP";
import verifyOtp from "./verifyOtp";
import resetPassword from "./resetPassword";

const router = Router();

router.post("/", post);
router.post("/resendOtp", resendOTP);
router.post("/verifyOtp", verifyOtp);
router.post("/resetPassword", resetPassword);

export default router;
