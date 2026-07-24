import { Router } from "express";
import multer from "multer";
import { getKeysData } from "./get";
import checkout from "./post";
import update from "./put";
import retKey from "./return";
import webhook from "./webhook";
import nonVendorWebhook from "./nonVendorWebhook";
import { authOnly } from "../../middlewares/restrict";

const upload = multer();
const router = Router();

// Public webhook routes
router.post("/webhook", upload.none(), webhook); // for vendor
router.post("/webhook/non-vendor", upload.none(), nonVendorWebhook); // for non-vendor

// Protected routes
router.get("/", authOnly, getKeysData);
router.post("/checkout", authOnly, checkout);
router.put("/:id", authOnly, update);
router.post("/:id/return", authOnly, retKey);

export default router;
