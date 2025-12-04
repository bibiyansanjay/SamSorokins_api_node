import { Router } from "express";
import post from "./post";
import get from "./get";
import extractParam from "../../middlewares/extractParam";
import videoId from "./videoId";

const router = Router();
router.post("/", post);
router.get("/", get);
//
router.use("/:videoId", extractParam("videoId"), videoId);

export default router;
