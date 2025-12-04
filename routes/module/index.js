import { Router } from "express";
import post from "./post";
import get from "./get";
import extractParam from "../../middlewares/extractParam";
import moduleId from "./moduleId";

const router = Router();
router.post("/", post); //create
router.get("/", get); //get all wd filters
router.use("/:moduleId", extractParam("moduleId"), moduleId); //by id

export default router;
