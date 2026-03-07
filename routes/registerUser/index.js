import Joi from "joi";
import { User } from "../../models";

const schema = Joi.object({
  firstName: Joi.string().required(),
  lastName: Joi.string().allow(""),
  email: Joi.string().required(),
  password: Joi.string().required(),
  address: Joi.string().allow(""),
  city: Joi.string().allow(""),
  country: Joi.string().required(),
  state: Joi.string().allow(""),
  phoneNumber: Joi.string(),
  pinCode: Joi.string().allow(""),
  preferredLanguage: Joi.string().required(),
  gender: Joi.string().valid("male", "female", "other").optional(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    const data = await schema.validateAsync(body);
    const userExist = await User.findOne({ email: data.email });

    if (!userExist) {
      const users = new User(data);

      //   const email = data?.email;
      //   encodedEmail = encodeURIComponent(email);
      //   const challenge = crypto.randomBytes(128).toString("hex");
      //   const resetTokenExpires = Date.now() + 86400000;
      //   users.resetPasswordToken = challenge;
      //   users.resetPasswordExpires = resetTokenExpires;
      //   const admin = User.findOne({ role: "ADMIN" });
      //   users.createdBy = admin?._id;

      // Prepare

      await users.save();
      return res.json({ message: "userRegistered", users });
    } else {
      return res.status(409).json({ message: "emailAlreadyExists" });
    }
  } catch (error) {
    console.log("Error adding user", error);
    next(error);
  }
};
