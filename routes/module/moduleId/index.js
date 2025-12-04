import { Router } from "express";
import put from "./put";
import get from "./get";
import _delete from "./delete";
import restore from "./restore";
import activate from "./activate";
import tutorial from "./tutorial";

const router = Router();

router.put("/", put); //update by id
router.get("/", get); //only for get by id and no filters or sort
router.delete("/", _delete);
router.put("/restore", restore);
router.put("/activate", activate);
router.get("/tutorial", tutorial); //only for get by id and no filters or sort

export default router;
