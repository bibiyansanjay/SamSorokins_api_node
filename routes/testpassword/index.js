import { Router } from "express";
import passwordHash from "../../models/users/pre/save/passwordHash";
const router = Router();

router.post("/", async (req, res) => {
  try {
    const password = req.body.password;
    if (!password) {
      return res.status(400).send("Password is required");
    }
    const hashed_password = await passwordHash(password); // Replace with actual hashing logic
    console.log("hashed_password", hashed_password);
    if (!hashed_password) {
      return res.status(500).send("Error hashing password");
    }
    // res.send("testpassword", hashed_password).json();
    return res.status(200).json({ "hashed_password": hashed_password });
  } catch (error) {
    console.log("error", error);
    next(error);
  }
});
export default router;
