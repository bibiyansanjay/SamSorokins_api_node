import Joi from "joi";
import sendOTP from "./sendOTP";
import bcrypt from "bcrypt"; // For password comparison
import getRecord from "../../utils/getRecord"; // Assuming this is a method to fetch records

const schema = Joi.object({
  email: Joi.string().required(),
  password: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    const validSchema = schema;
    const data = await validSchema.validateAsync(body);
    const { email, password } = data;
    // Query the database for the user
    console.log("Login attempt for email:", email);
    const rows = await getRecord(
      "user",
      { email: email },
      {
        selectFields: ["email", "password", "isActive", "isDeleted"],
      }
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: "User not found" });
    }
    console.log("Post - User found", rows);
    const user = rows[0];

    if (user.isDeleted === 1 || user.isActive === 0) {
      return res.status(403).json({ message: "User is inactive or deleted" });
    }
    const dbPassword = user.password;
    const enteredPassword = password; // Password entered by the user
    // Check password
    const checkPassword = await bcrypt.compare(enteredPassword, dbPassword);

    if (!checkPassword) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    await sendOTP(user);

    return res.json({
      message: "Otp Sent to your registered email",
      email: user.email,
    });
  } catch (error) {
    console.log("Post - Error while login", error);
    next(error);
  }
};
