import Joi from "joi";
import getRecord from "../../utils/getRecord";
import updateRecord from "../../utils/updateRecord";
import passwordHash from "../../models/users/pre/save/passwordHash";

const schema = Joi.object({
  email: Joi.string().required(),
  password: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    const { email, password } = await schema.validateAsync(body);

    // 1. Fetch user from MySQL
    const userData = await getRecord("user", { email: email });

    if (!userData || userData.length === 0) {
      return res.status(404).json({ message: "User Not Found" });
    }

    const user = userData[0];

    if (user.isDeleted === 1 || user.isActive === 0) {
      return res.status(403).json({ message: "User is inactive or deleted" });
    }
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    // Hash the new password
    const hashedNewPassword = await passwordHash(password);

    const result = await updateRecord(
      "user",
      { password: hashedNewPassword },
      { id: user.id }
    );

    if (result.affectedRows === 0) {
      return res.status(500).json({ message: "Password change failed" });
    }

    return res.json({ message: "Password Changed Successfully" });
  } catch (error) {
    res.json({
      error: error.message ? error.message : "Internal Server Error",
    });
  }
};
