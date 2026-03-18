import { Router } from "express";
import tus from "./tus";
import simpleUpload from "./simpleUpload";
import registerBatch from "./registerBatch";

const router = Router();

router.use("/registerBatch", registerBatch);

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
