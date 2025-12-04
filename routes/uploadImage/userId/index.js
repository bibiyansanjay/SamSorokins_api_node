import { Router } from "express";
import profilePicture from "./profilePicture";

const router = Router();

router.post("/profilePicture", profilePicture); //update by id

export default router;
