/**
 * @method /users/userId/change-password PUT
 * @memberof module:Routes.users
 * @description Function to handle PUT requests if User wants to change password.
 */

import Joi from "joi";
import { User } from "../../../../models";

const schema = Joi.object({
  // currentPassword: Joi.string().required(),
  newPassword: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    const { body, userId, user: requestingUser } = req;

    const data = await schema.validateAsync(body);
    const { newPassword } = data;

    const user = await User.findOne({ _id: userId, isDeleted: false });

    if (!user) {
      return res.status(404).json({ message: "User Not Found" });
    }

    // const matchPassword = await user.comparePassword(currentPassword);

    // if (!matchPassword) {
    //   return res.status(409).json({ message: "Invalid Current Password" });
    // }

    user.password = newPassword;
    user.resendOtpCount = 0;
    await user.save();

    return res.json({ message: "Password Changed Successfully" });
  } catch (error) {
    next(error);
  }
};
