import { Router } from "express";
import put from "./put";
import get from "./get";
import _delete from "./delete";
import restore from "./restore";
import activate from "./activate";
import getPermission from "./permission get";
import createPermission from "./permission create";

const router = Router();

router.put("/", put); //update by id
router.get("/", get); //only for get by id and no filters or sort
router.delete("/", _delete);
router.put("/restore", restore);
router.put("/activate", activate);
router.get("/permission", getPermission);
router.post("/permission", createPermission);

export default router;
// This code defines the routes for handling operations on a single user identified by userId.
// It includes routes for updating, retrieving, deleting, restoring, and activating a user.
