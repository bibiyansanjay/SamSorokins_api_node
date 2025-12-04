import { Router } from "express";
import test from "./test";
import testpassword from "./testpassword";
import registerUser from "./registerUser";
import auth from "./auth";
import forgotPassword from "./forgotPassword";
import users from "./users";
import role from "./role";
import module from "./module";

import jotform from "./jotform";
import uploadImage from "./uploadImage";
import { authOnly } from "../middlewares/restrict";
import { postman } from "../middlewares/postman";
import pages from "./pages";
import videoCategories from "./videoCategories";
import videos from "./videos";
import uploadCsv from "./uploadCsv";
import uploadVideos from "./uploadVideos";

const router = Router();
//
router.use("/test", test);
router.use("/testpassword", testpassword);
//
router.use("/role", postman, role);
router.use("/module", module);
//
router.use("/auth", auth);
router.use("/registerUser", postman, registerUser);
router.use("/forgotPassword", postman, forgotPassword);
router.use("/users", authOnly, users);
router.use("/upload", postman, uploadVideos);

//
router.use("/uploadImage", uploadImage);
// router.use("/jotform", postman, jotform);
// router.use("/uploadCsv", postman, uploadCsv);
//
// router.use("/pages", postman, pages);
// router.use("/videoCategories", postman, videoCategories);
// router.use("/videos", postman, videos);

// router.use("/chatBot", chatBot);
// router.use("/subscription", subscription);

export default router;
