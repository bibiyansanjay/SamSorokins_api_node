import Joi from "joi";
import passwordHash from "../../models/users/pre/save/passwordHash";
import createRecord from "../../utils/createRecord";

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
  role: Joi.string().required(),
  isDeleted: Joi.boolean(),
  isActive: Joi.boolean(),
  gender: Joi.string().valid("male", "female", "other").optional(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    const data = await schema.validateAsync(body);
    const hashedPassword = await passwordHash(data.password);
    data["password"] = hashedPassword;
    const result = await createRecord("user", data, {});
    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "User creation failed" });
    }
    return res.json({
      message: "User Registered Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    console.log("Error adding user", error);
    if (error.code === "ER_DUP_ENTRY") {
      return res.status(404).json({ message: "Email already exists" });
      //return res.status(409).json({ message: "emailAlreadyExists" });
    }
    next(error);
  }
};
