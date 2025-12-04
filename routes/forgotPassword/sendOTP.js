import sendMailAWS from "../../methods/sendMailAWS";
import generateOtp from "../../methods/generateOtp";
import updateRecord from "../../utils/updateRecord";

const sendOTP = async (user) => {
  const otp = await generateOtp();

  const result = await updateRecord(
    "user", //table name
    {
      otp: otp,
    },
    { email: user.email }
  );
  if (result.affectedRows === 0) {
    console.log("Otp Save Failed");
    return res.status(404).json({ message: "Server Error" });
  }
  console.log("sendOTP - user otp updated successfully");
  // Prepare payload for email generation
  const payload = {
    name: user?.firstName,
    email: user?.email,
    otp: otp,
  };

  const subject = "Forgot Password Authentication";
  const recipient = user?.email;
  const templateName = "template-forgotPasswordOtp";

  await sendMailAWS(subject, payload, recipient, templateName);
};

export default sendOTP;
