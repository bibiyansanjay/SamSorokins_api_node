import { Router } from "express";
import post from "./post";
import get from "./get";
import extractParam from "../../middlewares/extractParam";
import userId from "./userId";

/**
 * @namespace users
 * @memberof module:Routes
 * @description Defines all users routes.
 */

const router = Router();
router.post("/register", post);
router.get("/", get);
router.use("/:userId", extractParam("userId"), userId);

export default router;
