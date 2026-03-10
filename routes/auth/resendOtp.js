import Joi from "joi";
import { User } from "../../models";
import generateOtp from "../../methods/generateOtp";
import sendMail from "../../methods/sendMail";

const schema = Joi.object({
  email: Joi.string().email().required(),
});

const MAX_RESEND = 3;

export default async (req, res, next) => {
  try {
    const { body } = req;
    const OTP_EXPIRY_MINUTES = process.env.OTP_EXPIRY_MINUTES;

    const validSchema = schema;
    const data = await validSchema.validateAsync(body);
    const { email } = data;

    const user = await User.findOne({ email: email, isDeleted: false });

    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    if (!user?.isActive) {
      return res.status(429).json({
        message: "The user is inactive. Please contact administrator",
      });
    }
    // ⛔ Resend limit check
    if (user.resendOtpCount >= MAX_RESEND) {
      user.isActive = false;
      user.resendOtpCount = 0;
      user.otp = null;
      await user.save();

      return res.status(429).json({
        message: "The user is inactive. Please contact administrator",
      });
    }

    const otp = await generateOtp();

    user.otp = otp;
    user.otpExpiresAt = new Date(Date.now() + 5 * 60 * 1000);
    user.resendOtpCount += 1;
    user.lastActive = new Date();

    await user.save();

    //send mail
    const payload = {
      name: user?.firstName,
      email: user?.email,
      otp: otp,
    };

    const subject = "Two Factor Authentication";
    const recipient = user?.email;
    const templateName = "template-sendOtp";

    await sendMail(subject, payload, recipient, templateName);

    //"OTP resent successfully"
    return res.json({
      message: "Otp Sent to your registered email",
      email: user.email,
      resendCount: user.resendOtpCount,
    });
  } catch (error) {
    console.log("Error while verifying otp", error);
    next(error);
  }
};
