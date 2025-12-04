import { Router } from "express";
import put from "./put";
import _delete from "./delete";

const router = Router();

router.put("/", put);
router.delete("/", _delete);
export default router;
