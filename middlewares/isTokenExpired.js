// middleware/auth.js
import jwt from "jsonwebtoken";
import getRecord from "../utils/getRecord"; // Assuming this is a method to fetch records
import updateRecord from "../utils/updateRecord";

const isTokenExpired = async (req, res, next) => {
  //console.log("isTokenExpired");
  // const token = req.headers.authorization?.split(" ")[1];
  const authHeader = req.headers.authorization || "";
  const token = authHeader.startsWith("Bearer ")
    ? authHeader.split(" ")[1]
    : null;
  //   if (!token) return res.status(401).json({ message: "No token provided" });
  if (!token || token === "undefined") {
    return next(); //  IMPORTANT: allow public routes to proceed
  }

  // if (token) {
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    //const user = await User.findById(decoded?.id);
    const rows = await getRecord("user", { id: decoded?.id });
    const user = rows[0];
    //console.log("User found in isTokenExpired middleware", user);
    if (rows.length === 0) {
      return res.status(404).json({ message: "User not found" });
    }
    if (!user) return res.status(404).json({ message: "User not found" });

    const now = new Date();
    const diffInMs = now - user.lastActive;
    const diffInHours = diffInMs / (1000 * 60 * 60);

    if (diffInHours > 24) {
      //1week 168
      return res
        .status(401)
        .json({ message: "Session expired due to inactivity" });
    }
    const shouldUpdate = now - user.lastActive > 5 * 60 * 1000; // 5 minutes

    // Update lastActive on every request
    if (shouldUpdate) {
      await updateRecord(
        "user",
        {
          lastActive: now,
        },
        { email: user.email }
      );
    }
    req.user = user;
    next();
  } catch (err) {
    return res.status(401).json({ message: "Invalid or expired token" });
    // }
  }
};

module.exports = isTokenExpired;
