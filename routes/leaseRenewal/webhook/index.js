import { Router } from "express";
import multer from "multer";
import post from "./post";

const upload = multer();
const router = Router();

router.post("/", upload.none(), post);

export default router;
