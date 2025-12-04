import sendMailAWS from "../../methods/sendMailAWS";
import generateOtp from "../../methods/generateOtp";
import updateRecord from "../../utils/updateRecord";
import sendMailGmail from "../../methods/sendMailGmail";

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
    return res
      .status(404)
      .json({ message: "sendOTP - user otp update failed" });
  }
  console.log("sendOTP - user otp updated successfully");
  // Prepare payload for email generation
  const payload = {
    name: user?.firstName,
    email: user?.email,
    otp: otp,
  };

  const subject = "Two Factor Authentication";
  const recipient = user?.email;
  const templateName = "template-sendOtp";

  await sendMailAWS(subject, payload, recipient, templateName);
  //await sendMailGmail(subject, payload, recipient, templateName);
};

export default sendOTP;
