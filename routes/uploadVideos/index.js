import { Router } from "express";
import tus from "./tus";
import simpleUpload from "./simpleUpload";
import registerBatch from "./registerBatch";

const router = Router();

router.use("/registerBatch", registerBatch);
router.use("/tus", tus);
// router.all("/tus/*", tus);
// ✅ FIXED
// router.all("/tus/:path(*)", tus);
router.use("/", simpleUpload);

export default router;
