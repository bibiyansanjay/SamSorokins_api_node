import { Router } from "express";
import test from "./test";
import registerUser from "./registerUser";
import auth from "./auth";
import forgotPassword from "./forgotPassword";
import users from "./users";
import { authOnly } from "../middlewares/restrict";
import { postman } from "../middlewares/postman";
import uploadVideos from "./uploadVideos";
import jotform from "./jotform";
// import chatBot from "./chatBot";
// import subscription from "./subscription";
/**
 * @module Routes
 * @description Sets up routing for the Express server with various endpoints.
 */

const router = Router();

router.use("/test", test);
router.use("/registerUser", registerUser);
router.use("/auth", auth);
router.use("/jotform", jotform);
router.use("/forgotPassword", forgotPassword);
router.use("/users", authOnly, users);
router.use("/upload", postman, uploadVideos);
// router.use("/chatBot", chatBot);
// router.use("/subscription", subscription);

export default router;
