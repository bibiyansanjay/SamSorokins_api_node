import { Router } from "express";
import multer from "multer";
import put from "./put";

const upload = multer({ storage: multer.memoryStorage() });
const router = Router();

router.put("/", upload.single("file"), put);

export default router;
