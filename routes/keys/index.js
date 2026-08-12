import { Router } from "express";
import multer from "multer";
import { getKeysData } from "./get";
import checkout from "./post";
import update from "./put";
import retKey from "./return";
import deleteKey from "./delete";
import webhook from "./webhook";
import nonVendorWebhook from "./nonVendorWebhook";
import keyManagementWebhook from "./keyManagementWebhook";
import { authOnly } from "../../middlewares/restrict";

const upload = multer();
const router = Router();

// Public webhook routes
router.post("/webhook", upload.none(), webhook); // for vendor
router.post("/webhook/non-vendor", upload.none(), nonVendorWebhook); // for non-vendor
router.post("/webhook/key-management", upload.none(), keyManagementWebhook); // for key management

// Protected routes
router.get("/", authOnly, getKeysData);
router.post("/checkout", authOnly, checkout);
router.put("/:id", authOnly, update);
router.post("/:id/return", authOnly, retKey);
router.delete("/:id", authOnly, deleteKey);

export default router;
