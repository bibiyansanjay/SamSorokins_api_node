import Joi from "joi";
import bcrypt from "bcrypt";
import passwordHash from "../../../../models/users/pre/save/passwordHash";
import updateRecord from "../../../../utils/updateRecord";
import getRecord from "../../../../utils/getRecord";

const schema = Joi.object({
  currentPassword: Joi.string().required(),
  newPassword: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    const { body, userId } = req;
    const { currentPassword, newPassword } = await schema.validateAsync(body);

    // 1. Fetch user from MySQL
    const userData = await getRecord("user", { id: userId });

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

    // Compare currentPassword (plain text) with the hashed password in DB
    const isMatch = await bcrypt.compare(currentPassword, user.password);
    if (!isMatch) {
      return res.status(409).json({ message: "Invalid Current Password" });
    }
    // Hash the new password
    const hashedNewPassword = await passwordHash(newPassword);

    const result = await updateRecord(
      "user",
      { password: hashedNewPassword },
      { id: userId }
    );

    if (result.affectedRows === 0) {
      return res.status(500).json({ message: "Password update failed" });
    }

    return res.json({ message: "Password Changed Successfully" });
  } catch (error) {
    console.error("Error changing password:", error);
    next(error);
  }
};
