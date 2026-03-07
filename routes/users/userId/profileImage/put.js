/**
 * @method /users/userId/profile-img PUT
 * @memberof module:Routes.users
 * @description Function to handle PUT requests to update profile Image.
 */

import Joi from "joi";
import { User } from "../../../../models";

const schema = Joi.object({
  profileImg: Joi.string().allow("").allow(null),
  signature: Joi.string().allow("").allow(null),
});

export default async (req, res, next) => {
  try {
    const { body, userId } = req;
    const data = await schema.validateAsync(body);
    const user = await User.findOneAndUpdate(
      { _id: userId },
      { profileImg: data?.profileImg, signature: data?.signature }
    );

    if (!user) {
      return res.status(404).json({ message: "User Not Found" });
    }

    return res.json({ message: "Profile Image Updated Successfully" });
  } catch (error) {
    next(error);
  }
};
