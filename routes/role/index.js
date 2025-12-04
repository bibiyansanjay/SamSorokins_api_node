import { Router } from "express";
import post from "./post";
import get from "./get";
import extractParam from "../../middlewares/extractParam";
import roleId from "./roleId";

const router = Router();
router.post("/", post); //create
router.get("/", get); //get all wd filters
router.use("/:roleId", extractParam("roleId"), roleId); //by id

export default router;
