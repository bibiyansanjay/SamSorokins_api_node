import Joi from "joi";
import crypto from "crypto";
import sendMail from "../../methods/sendMail";
import { User } from "../../models";

/**
 * @name /forgot-password POST
 * @memberof module:Routes.forgotPassword
 * @description Function to handle POST requests if User forgot password.
 */

export default async (req, res, next) => {
  try {
    const {
      query: { email },
    } = req;
    console.log(process.env.REFRESH_TOKEN);
    console.log(email);
    const user = await User.findOne({ email: email });

    if (!user) {
      return res.status(404).json({ message: "Invalid email" });
    }
    // if (!user?.isActive) {
    //   return res.status(401).json({
    //     message: "User is InActive",
    //   });
    // }
    const encodedEmail = encodeURIComponent(email);

    //generate challenge
    const challenge = crypto.randomBytes(128).toString("hex");
    const resetTokenExpires = Date.now() + 86400000;
    user.resetPasswordToken = challenge;
    user.resetPasswordExpires = resetTokenExpires;
    await user.save();

    // Send email with password reset link
    const resetPasswordLink = `${process.env.REACT_APP_URL}/password/reset?address=${encodedEmail}&challenge=${challenge}`;

    // Prepare payload for PDF generation
    const payload = {
      //   body: updatedData,
      name: user?.firstName,
      email: user?.email,
      resetLink: resetPasswordLink,
    };
    // Ensure payload.body contains valid HTML
    const htmlBody = `
   <!DOCTYPE html>
   <html lang="en">
     <head>
       <meta charset="UTF-8">
       <meta name="viewport" content="width=device-width, initial-scale=1.0">
     </head>
     <body>
       ${payload.body}
     </body>
   </html>
   `;

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
