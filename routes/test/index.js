import { Router } from "express";
const router = Router();

router.get("/", (req, res) => {
  try {
    res.send("Testing done successfully.");
  } catch (error) {
    console.log("error", error);
    next(error);
  }
});
export default router;
