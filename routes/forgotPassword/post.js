import Joi from "joi";
import getRecord from "../../utils/getRecord";
import sendOTP from "./sendOTP";

const schema = Joi.object({
  email: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    const validSchema = schema;
    const data = await validSchema.validateAsync(body);

    const { email } = data;
    // Query the database for the user
    //console.log("Password Forgot attempt for email:", email);
    const rows = await getRecord(
      "user",
      { email: email },
      {
        selectFields: ["email", "isActive", "isDeleted"],
      }
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: "User not found" });
    }

    //console.log("Password Forgot - User found", rows);
    const user = rows[0];

    if (user.isDeleted === 1 || user.isActive === 0) {
      return res.status(403).json({ message: "User is inactive or deleted" });
    }
    await sendOTP(user);

    return res.json({
      message: "Otp Sent to your registered email",
      email: user.email,
    });
  } catch (error) {
    next(error);
  }
};
