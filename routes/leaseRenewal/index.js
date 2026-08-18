import { Router } from "express";
import webhook from "./webhook";

/**
 * @namespace leaseRenewal
 * @memberof module:Routes
 * @description Defines all lease renewal routes.
 */
const router = Router();

router.use("/webhook", webhook);

export default router;
