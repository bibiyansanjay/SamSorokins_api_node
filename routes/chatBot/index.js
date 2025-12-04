import { Router } from "express";
import post from "./post";

/**
 * @namespace chatBot
 * @memberof module:Routes
 * @description Defines all chatBot routes.
 */

const router = Router();

router.post("/", post);

export default router;
