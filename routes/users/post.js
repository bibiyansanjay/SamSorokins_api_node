import Joi from "joi";
import { User } from "../../models";
import crypto from "crypto";
import sendMail from "../../methods/sendMail";

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
  profileImg: Joi.string().allow(""),
  role: Joi.string().valid("CLIENT", "ADMIN", "SUB-ADMIN").required(),
  isDeleted: Joi.boolean(),
  // isActive: Joi.boolean(),
  // isLoggedIn: Joi.boolean(),
  gender: Joi.string().valid("male", "female", "other").optional(),
});

/**
 * @module RegisterUser
 * @description Register the new user here
 * @author Tushar Sharma
 */

export default async (req, res, next) => {
  try {
    // const { body, user } = req;
    const { body } = req;
    const data = await schema.validateAsync(body);

    const userExist = await User.findOne({
      email: data.email,
      isDeleted: false,
    });
    // let encodedEmail = "";

    if (!userExist) {
      // const users = new User(data);
      const users = new User(body);

      const email = data?.email;
      const encodedEmail = encodeURIComponent(email);
      const challenge = crypto.randomBytes(128).toString("hex");
      const resetTokenExpires = Date.now() + 86400000;
      users.resetPasswordToken = challenge;
      users.resetPasswordExpires = resetTokenExpires;
      users.isCreatePasswordMailSend = true;
      // const admin = User.findOne({ role: "ADMIN" });
      // users.createdBy = user?._id || admin?._id;
      const resetPasswordLink = `${process.env.REACT_APP_URL}/password/create?address=${encodedEmail}&challenge=${challenge}`;

      // Prepare

      const payload = {
        // body: updatedData,
        user: body,
        name: body?.firstName,
        resetLink: resetPasswordLink,
      };
      const subject =
        "Welcome to Custom Layout! Set Up Your Password and Get Started";
      const templateName = "template-newUser";
      await sendMail(subject, payload, email, templateName);

      //       users.isCreatePasswordMailSend = true;
      await users.save();
      return res.json({ message: "User Registered Successfully", users });
    } else {
      return res.status(409).json({ message: "Email Already Exist" });
    }
  } catch (error) {
    console.log("Error adding user", error);
    next(error);
  }
};
