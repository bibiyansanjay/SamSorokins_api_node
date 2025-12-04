import { Router } from "express";
import post from "./post";
import get from "./get";
import extractParam from "../../middlewares/extractParam";
import userId from "./userId";
import getClients from "./getClients";

const router = Router();
router.post("/register", post);
router.get("/", get);
router.get("/getClients", getClients);
router.use("/:userId", extractParam("userId"), userId);

export default router;
