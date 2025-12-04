import Joi from "joi";
import updateRecord from "../../utils/updateRecord";
import getRecord from "../../utils/getRecord";

const schema = Joi.object({
  email: Joi.string().required(),
  otp: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    //console.log(req.body);
    const { body } = req;
    const data = await schema.validateAsync(body);
    const { email, otp } = data;

    const userData = await getRecord("user", { email: email });
    if (!userData || userData.length === 0) {
      return res.status(404).json({
        success: false,
        message: "User not found",
        data: [],
      });
    }
    const user = userData[0];
    //console.log("Verify OTP - User found", user);
    if (user.isDeleted === 1 || user.isActive === 0) {
      return res.status(403).json({ message: "User is inactive or deleted" });
    }
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }
    if (user?.otp !== otp) {
      return res.status(400).json({ message: "Invalid OTP" });
    }
    const now = new Date();
    // Reset resend count & clear OTP after successful login
    const result = await updateRecord(
      "user",
      {
        otp: null,
        resendOtpCount: 0,
        //lastActive: now,
      },
      { email: user.email }
    );
    if (result.affectedRows === 0) {
      console.log("otp, count Save Failed");
      return res.status(404).json({ message: "Server Error" });
    }
    //console.log("Verify OTP - user updated successfully");
    //
    return res.json({ message: "OTP Verified Successfully", email });
  } catch (error) {
    console.log("Post - Error while verifying otp", error);
    next(error);
  }
};
