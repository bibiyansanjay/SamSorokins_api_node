import generateOtp from "../../methods/generateOtp";
import getRecord from "../../utils/getRecord";
import updateRecord from "../../utils/updateRecord";
import sendOTP from "./sendOTP";

const MAX_RESEND_COUNT = 3;

export default async (req, res, next) => {
  try {
    const { body } = req;
    const { email } = body;
    if (!email) {
      return res.status(400).json({ message: "Email is required" });
    }
    //
    const userData = await getRecord("user", { email: email });
    console.log("Resend OTP - userData", userData);
    if (!userData || userData.length === 0) {
      return res.status(404).json({
        success: false,
        message: "User not found",
        data: [],
      });
    }
    const user = userData[0];
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }
    console.log("Resend OTP -User found", user);
    // Check if user is deleted or inactive
    if (user.isDeleted === 1 || user.isActive === 0) {
      return res.status(403).json({ message: "User is inactive or deleted" });
    }

    const currentCount = user.resendOtpCount || 0;

    if (currentCount >= MAX_RESEND_COUNT) {
      console.log(
        `OTP resend limit reached for ${user.email} (count = ${currentCount})`
      );
      // Update user to inactive and reset resend count
      const result = await updateRecord(
        "user",
        {
          isActive: false,
          resendOtpCount: 0,
        },
        { email: email }
      );
      if (result.affectedRows === 0) {
        return res
          .status(404)
          .json({ message: "Resend OTP - user inactive, count update failed" });
      }
      console.log("Resend OTP - user inactive, count updated successfully");
      //
      return res.status(403).json({
        message: "OTP resend limit reached. Account Blocked Contact Admin!!",
        inActive: false,
      });
    }

    // Generate a new OTP
    const otp = await generateOtp();

    const result1 = await updateRecord(
      "user",
      {
        otp: otp,
        resendOtpCount: currentCount + 1,
      },
      { email: email }
    );
    if (result1.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Resend OTP - user otp,count update failed" });
    }
    console.log("Resend OTP - user otp,count updated successfully");
    // Very important → await sendOTP!
    await sendOTP(user);

    return res.json({
      message: "OTP Resent to registered email",
      resendOtpCount: currentCount + 1,
    });
  } catch (err) {
    next(err);
  }
};
