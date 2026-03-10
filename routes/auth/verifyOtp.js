import Joi from "joi";
import { User } from "../../models";

/**
 * @name /auth/verifyOtp POST
 * @memberof module:Routes.auth
 * @description Function to handle POST requests for login a user.
 */

const schema = Joi.object({
  email: Joi.string().required(),
  otp: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    const data = await schema.validateAsync(body);
    const { email, otp } = data;
    const user = await User.findOne({ email: email, isDeleted: false });

    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    // ✅ Check OTP expiration
    if (!user.otpExpiresAt || user.otpExpiresAt < new Date()) {
      return res
        .status(400)
        .json({ message: "OTP expired. Please request a new OTP." });
    }

    if (user?.otp !== otp) {
      return res.status(400).json({ message: "Invalid OTP" });
    }
    const token = user.getToken();
    user.otp = null;
    // user.isLoggedIn = true;
    user.otpExpiresAt = null;
    user.lastActive = new Date();
    user.resendOtpCount = 0;
    await user.save();

    return res.json({
      message: "User Logged In Successfully",
      token,
      token,
      user: {
        id: user._id,
        email: user.email,
        role: user.role,
        fullName: user?.fullName,
        firstName: user?.firstName,
        lastName: user?.lastName,
      },
    });
  } catch (error) {
    console.log("Error while verifying otp", error);
    next(error);
  }
};
