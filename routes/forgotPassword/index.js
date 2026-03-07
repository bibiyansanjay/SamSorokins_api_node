import { Router } from "express";
import post from "./post";
import resetPassword from "./resetPassword";

/**
 * @namespace forgotPassword
 * @memberof module:Routes
 * @description Defines all forgotPassword routes.
 */

const router = Router();

router.post("/", post);
router.post("/resetPassword", resetPassword);

export default router;
