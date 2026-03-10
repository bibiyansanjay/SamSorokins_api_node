import Joi from "joi";
import { User } from "../../../models";

const schema = Joi.object({
  firstName: Joi.string(),
  lastName: Joi.string(),
  email: Joi.string(),
  phoneNumber: Joi.string(),
  country: Joi.string(),
  state: Joi.string().allow(""),
  city: Joi.string().allow(""),
  address: Joi.string().allow(""),
  pinCode: Joi.string().allow(""),
  profileImg: Joi.string().allow(""),
  role: Joi.string().valid("Client", "Admin"),
  preferredLanguage: Joi.string(),
  isDeleted: Joi.boolean(),
  isActive: Joi.boolean(),
  isLoggedIn: Joi.boolean(),
  gender: Joi.string().valid("male", "female", "other").optional(),
});

export default async (req, res, next) => {
  try {
    const { body, userId } = req;
    const data = await schema.validateAsync(body);

    const isUserExist = await User.findOne({ _id: userId, isDeleted: false });

    if (!isUserExist) {
      return res.status(404).json({ message: "User Not Exist." });
    }

    // data.resendOtpCount = 0;
    const user = await User.findOneAndUpdate({ _id: userId }, data, {
      new: true,
    });

    return res.json({
      message: "User Updated successfully",
      user: {
        id: user._id,
        email: user.email,
        role: user.role,
        fullName: user?.fullName,
        firstName: user?.firstName,
        lastName: user?.lastName,
      },
    });
  } catch (error) {
    next(error);
  }
};
