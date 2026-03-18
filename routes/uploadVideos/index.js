import { Router } from "express";
import tus from "./tus.js";
import simpleUpload from "./simpleUpload.js";
import registerBatch from "./registerBatch.js";
import download from "./download.js";

const router = Router();

router.use("/registerBatch", registerBatch);
router.use("/download", download);

// tus is now the getTusServer async function
let tusInstance = null;
router.use("/tus", async (req, res, next) => {
  try {
    if (!tusInstance) {
      tusInstance = await tus(); // Initialize once
    }
    // Pass raw req/res directly so @tus/server can access res.writeHead 
    // natively without being mangled by Express's async wrapper chain
    return tusInstance.handle(req, res);
  } catch (error) {
    next(error);
  }
});

router.use("/", simpleUpload);

export default router;
