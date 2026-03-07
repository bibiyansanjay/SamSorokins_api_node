// import { isMoreThan24Hours } from "../methods/isMoreThan24Hours";
import jwt from "jsonwebtoken";
import { User } from "../models";

/**
 * @module Middleware
 * @description All middleware functions define here.
 */

/**
 *
 * @method getToken
 * @description Extracts the Bearer token from the Authorization header.
 *
 */

const getToken = (req, res) => {
  const authHeader = req.headers["authorization"];

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).send({ message: "Unauthorized" });
  }

  const token = authHeader && authHeader.split(" ")[1];

  if (!token) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  return token;
};

/**
 * @method authOnly
 * @description Verifies JWT token, checks if user is active, and attaches user to request object.
 */

export const authOnly = async (req, res, next) => {
  try {
    // Get the token
    const token = getToken(req, res);
    if (!token) {
      return res.status(401).json({ message: "Token not found" });
    }

    // Verify the token
    let decoded;
    try {
      decoded = jwt.verify(token, process.env.JWT_SECRET);
    } catch (err) {
      return res.status(401).json({ message: "Invalid or expired token" });
    }

    const { id } = decoded;

    // Find the user
    const user = await User.findOne({
      _id: id,
      isDeleted: false,
    });

    if (!user) {
      return res.status(401).json({ message: "Unauthorized user" });
    }

    // Check if user was last active more than 24 hours ago
    // const lastActive = user?.lastActive;
    // const isMoreThan24Hour = isMoreThan24Hours(lastActive);

    // if (lastActive && isMoreThan24Hour) {
    //   return res.status(401).json({
    //     message: "Invalid or expired token",
    //   });
    // }

    // Update last active date
    // user.lastActive = new Date();
    await user.save();

    // Attach user to request object
    req.user = user;

    next();
  } catch (error) {
    return res.status(500).json({ message: "Server error", error });
  }
};

/**
 * @method adminOnly
 * @description Verifies JWT token and checks if the user is an admin.
 */

export const adminOnly = async (req, res, next) => {
  const token = getToken(req, res);

  const { id } = jwt.verify(token, process.env.JWT_SECRET);

  const user = await User.findOne({
    _id: id,
    role: "ADMIN",
    isDeleted: false,
  });

  if (!user) {
    return res.status(401).json({ message: "Unauthorized user" });
  }

  req.user = user;

  next();
};
