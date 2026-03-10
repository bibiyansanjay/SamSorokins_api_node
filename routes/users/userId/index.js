import { Router } from "express";
import put from "./put";
import get from "./get";
import passwordChange from "./passwordChange";
import profileChange from "./profileImage";
// import { authOnly } from "../../../middlewares/restrict";

import _delete from "./delete";
import restore from "./restore";

const router = Router();

/**
 * @namespace userId
 * @memberof module:Routes
 * @description Defines all single user routes.
 */

router.use("/change-password", passwordChange);
router.use("/profileImg", profileChange);
router.put("/restore-user", restore);

router.put("/", put);
router.get("/", get);
router.delete("/", _delete);

export default router;
