import jwt, { JsonWebTokenError, TokenExpiredError } from "jsonwebtoken";
import { User } from "../../models";

/**
 * @name /forgot-password/resetPassword POST
 * @memberof module:Routes.forgotPassword
 * @description Function to handle POST requests to reset Password.
 * @body {string} newPassword - The new password to set
 * @body {string} challenge - The JWT reset token sent via email
 */

export default async (req, res, next) => {
  try {
    const { newPassword, challenge } = req.body;

    // 1. Validate required fields
    if (!challenge || !newPassword) {
      return res
        .status(400)
        .json({ message: "Token and password are required" });
    }

    // 2. Validate password strength (optional but recommended)
    if (newPassword.length < 8) {
      return res
        .status(400)
        .json({ message: "Password must be at least 8 characters long" });
    }

    // 3. Verify the JWT token
    let payload = "";
    try {
      payload = jwt.verify(challenge, process.env.JWT_SECRET);
    } catch (err) {
      if (err instanceof TokenExpiredError) {
        return res.status(410).json({
          message:
            "Oops! This reset link is no longer valid. Please request a new one.",
        });
      }
      if (err instanceof JsonWebTokenError) {
        return res.status(401).json({ message: "Invalid token" });
      }
      throw err; // re-throw unexpected errors to outer catch
    }

    const userId = payload.sub;

    // 4. Find user by ID
    const user = await User.findOne({ _id: userId });

    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    // 5. Check token matches what's stored in DB (prevents token reuse)
    if (user.resetPasswordToken !== challenge) {
      return res.status(401).json({
        message:
          "This reset link has already been used. Please request a new one.",
      });
    }

    // 6. Hash the new password before saving
    // const saltRounds = 10;
    // const hashedPassword = await bcrypt.hash(newPassword, saltRounds);

    // 7. Update user — clear reset token fields
    user.password = newPassword;
    user.resetPasswordToken = null;
    user.resetPasswordExpires = null;

    await user.save();

    return res.status(200).json({
      message: "Password reset successfully",
    });
  } catch (error) {
    console.error("[resetPassword] Unexpected error:", error);
    return res.status(500).json({
      message: error.message ? error.message : "Internal Server Error",
    });
  }
};
