import generateOtp from "../../methods/generateOtp";
import sendMail from "../../methods/sendMail";

/**
 * @Api /sendOTP
 *
 * @Method POST
 *
 * @Description send OTP to users
 */

const sendOTP = async (user) => {
  const otp = await generateOtp();
  user.otp = otp;
  user.lastActive = new Date();
  await user.save();

  // const templateDoc = await Template.findOne({ id: "sendOtp" });
  // if (!templateDoc) {
  //   throw new Error("Template not found");
  // }
  // Replace placeholders in the template content
  // const updatedData = getTemplateData(user?.preferredLanguage,templateDoc)
  //   .replace(/\[name\]/g, user?.firstName)
  //   .replace(/\[otp\]/g, otp);

  // Prepare payload for PDF generation
  const payload = {
    name: user?.firstName,
    email: user?.email,
    otp: otp,
  };

  const subject = "Two Factor Authentication";
  const recipient = user?.email;
  const templateName = "template-sendOtp";

  await sendMail(subject, payload, recipient, templateName);
};

export default sendOTP;
