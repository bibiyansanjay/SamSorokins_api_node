import { Router } from "express";
import tus from "./tus.js";
import simpleUpload from "./simpleUpload.js";
import registerBatch from "./registerBatch.js";
import download from "./download.js";
import downloadZip from "./downloadZip.js";
import deleteFile from "./deleteFile.js";
import deleteByName from "./deleteByName.js";
import deleteBatch from "./deleteBatch.js";
import updateStatus from "./updateStatus.js";
import uploadThumbnail from "./uploadThumbnail.js";

const router = Router();

router.use("/registerBatch", registerBatch);
router.use("/download", download);
router.use("/downloadZip", downloadZip);
router.use("/deleteFile", deleteFile);
router.use("/deleteByName", deleteByName);
router.use("/deleteBatch", deleteBatch); // to delete files on browser refresh/ close
router.use("/updateStatus", updateStatus);
router.use("/thumbnail", uploadThumbnail);

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
