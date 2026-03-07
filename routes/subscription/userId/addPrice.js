import Joi from "joi";
import User from "../../../models/users";

/**
 * @name /users/:userId/restore-user PUT
 * @memberof module:Routes.formBuilder
 * @description Function to handle put request for user to restore user.
 */

export default async (req, res, next) => {
  try {
    const { userId, body } = req;

    const updateUser = await User.findOneAndUpdate(
      { _id: userId },
      { $inc: { walletPrice: body?.walletPrice } },
      { new: true }
    );

    res.json({ message: "Price added successfully", updateUser });
  } catch (error) {
    next(error);
  }
};
