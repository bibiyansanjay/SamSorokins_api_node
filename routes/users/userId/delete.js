import { User } from "../../../models";

export default async (req, res, next) => {
  try {
    const { userId } = req;
    await User.updateOne({ _id: userId }, { isDeleted: true });

    return res.json({ message: "User Deleted Successfully" });
  } catch (error) {
    next(error);
  }
};
