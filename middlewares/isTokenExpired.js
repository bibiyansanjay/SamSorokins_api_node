// middleware/auth.js

import { User } from "../models";
import jwt from "jsonwebtoken";

const isTokenExpired = async (req, res, next) => {
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
    const user = await User.findById(decoded?.id);

    if (!user) return res.status(404).json({ message: "User not found" });

    const now = new Date();
    const diffInMs = now - user.lastActive;
    const diffInHours = diffInMs / (1000 * 60 * 60);

    if (diffInHours > 24) {
      return res
        .status(401)
        .json({ message: "Session expired due to inactivity" });
    }
    const shouldUpdate = now - user.lastActive > 5 * 60 * 1000; // 5 minutes

    // Update lastActive on every request
    // user.lastActive = now;
    // await user.save();
    if (shouldUpdate) {
      user.lastActive = now;
      await user.save();
    }
    req.user = user;
    next();
  } catch (err) {
    return res.status(401).json({ message: "Invalid or expired token" });
    // }
  }
};

module.exports = isTokenExpired;
