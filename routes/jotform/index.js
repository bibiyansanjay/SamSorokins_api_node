import { Router } from "express";
import webhook from "./webhook";

const router = Router();
router.post("/webhook/", webhook); //create

export default router;
