import Joi from "joi";
import { User } from "../../models";
import sendOTP from "./sendOTP";

/**
 * @name /auth POST
 * @memberof module:Routes.auth
 * @description Function to handle POST requests for login a user.
 */
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
    const user = await User.findOne({ email: email, isDeleted: false });

    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }
    const checkPassword = await user.comparePassword(password);
    if (!checkPassword) {
      return res.status(401).json({ message: "invalidCredentials" });
    }
    // if (!user?.isActive) {
    //   return res.status(401).json({
    //     message: res.__("userInactive"),
    //   });
    // }
    // if (!!user?.permissions?.twoFactorAuthentication) {
    sendOTP(user);
    return res.json({
      message: "Otp Sent to your registered email",
      email: user.email,
    });
    // }
    // const token = user.getToken();
    // user.lastActive = new Date();
    // user.otp = null;
    // user.isLoggedIn = true;
    // await user.save();
    // return res.json({ message: "userLoggedIn", token });
  } catch (error) {
    console.log("Error while login", error);
    next(error);
  }
};
