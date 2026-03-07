import { User } from "../../models";

/**
 * @name /forgot-password/resetPassword POST
 * @memberof module:Routes.forgotPassword
 * @description Function to handle POST requests to reset Password.
 */

export default async (req, res, next) => {
  try {
    const { email, newPassword, challenge, isCreatingNewPassword } = req.body;

    const user = await User.findOne({
      email: email,
      resetPasswordToken: challenge,
      resetPasswordExpires: { $gt: Date.now() },
    });

    if (!user) {
      return res.status(404).json({ message: "User not found!" });
    }

    user.password = newPassword;
    user.resetPasswordToken = "";
    user.resetPasswordExpires = "";

    await user.save();

    return res.json({
      message: isCreatingNewPassword
        ? "Password Created Successfully"
        : "Password Reseted successfully",
    });
  } catch (error) {
    res.json({
      error: err.message ? err.message : "Internal Server Error",
    });
  }
};
