import { Router } from "express";
import put from "./put";
import get from "./get";
import passwordChange from "./passwordChange";
// import { authOnly } from "../../../middlewares/restrict";

import _delete from "./delete";
import restore from "./restore";

const router = Router();

router.use("/change-password", passwordChange);
router.put("/restore", restore);

router.put("/", put);
router.get("/", get);
router.delete("/", _delete);

export default router;
