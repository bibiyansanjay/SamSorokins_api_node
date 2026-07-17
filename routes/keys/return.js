import { Key } from "../../models";

export default async (req, res, next) => {
  try {
    const { id } = req.params;

    const key = await Key.findOne({ _id: id, isReturned: false });
    if (!key) {
      return res
        .status(404)
        .json({ message: "Active key checkout not found." });
    }

    key.isReturned = true;
    key.returnedAt = new Date();
    key.accessLog.push(
      `${new Date().toLocaleString()}: Key returned/checked in.`
    );

    await key.save();
    return res.json({ message: "Key returned successfully", key });
  } catch (error) {
    next(error);
  }
};
