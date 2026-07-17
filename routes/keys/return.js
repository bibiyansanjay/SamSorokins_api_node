import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

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

    // Notify status change (returned)
    const keyCopy = {
      ...key.toObject(),
      status: "Returned / Checked In"
    };
    sendKeyStatusEmail(keyCopy); // send email in background

    return res.json({ message: "Key returned successfully", key });
  } catch (error) {
    next(error);
  }
};
