import Joi from "joi";
import crypto from "crypto";
import sendMail from "../../methods/sendMail";
import { User } from "../../models";

import jwt from "jsonwebtoken";

/**
 * @name /forgot-password POST
 * @memberof module:Routes.forgotPassword
 * @description Function to handle POST requests if User forgot password.
 */

const RESET_SECRET = process.env.JWT_SECRET;

const generateResetToken = (userId) => {
  return jwt.sign(
    {
      sub: userId,
      purpose: "reset-password",
    },
    RESET_SECRET,
    { expiresIn: "1h" }
  );
};

export default async (req, res, next) => {
  try {
    const { body } = req;
    const { email } = body;
    // console.log({ email }, { body });
    const user = await User.findOne({ email: email });

    if (!email) {
      return res.status(404).json({ message: "Email is required" });
    }

    if (!user) {
      return res.status(404).json({ message: "Invalid email" });
    }

    if (!user?.isActive) {
      return res.status(401).json({
        message: "User is InActive",
      });
    }

    // const encodedEmail = encodeURIComponent(email);
    const token = generateResetToken(user._id);
    //generate challenge
    // const challenge = crypto.randomBytes(128).toString("hex");
    // const resetTokenExpires = Date.now() + 86400000;
    user.resetPasswordToken = token;
    // user.resetPasswordExpires = resetTokenExpires;
    await user.save();

    // Send email with password reset link
    const resetPasswordLink = `${process.env.REACT_APP_URL}/password/reset?challenge=${token}`;

    // Prepare payload for PDF generation
    const payload = {
      //   body: updatedData,
      name: user?.firstName,
      email: user?.email,
      resetLink: resetPasswordLink,
    };

    // Send email with password reset link
    const subject = "Forgot password link";

    const recipient = email;
    const templateName = "template-forgotPassword";

    await sendMail(subject, payload, recipient, templateName);

    return res.json({
      message: "Forgot password link sent",
    });
  } catch (error) {
    next(error);
  }
};
