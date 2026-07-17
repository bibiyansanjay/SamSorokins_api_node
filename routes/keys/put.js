import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

export default async (req, res, next) => {
  try {
    const { id } = req.params;
    const { whoHasIt, status } = req.body;

    const key = await Key.findOne({ _id: id, isReturned: false });
    if (!key) {
      return res
        .status(404)
        .json({ message: "Active key checkout not found." });
    }

    let isChanged = false;
    let statusChanged = false;

    if (
      whoHasIt !== undefined &&
      whoHasIt.trim() !== "" &&
      whoHasIt !== key.whoHasIt
    ) {
      const oldPossessor = key.whoHasIt;
      key.whoHasIt = whoHasIt;
      key.accessLog.push(
        `${new Date().toLocaleString()}: Transferred from ${oldPossessor} to ${whoHasIt}`
      );
      isChanged = true;
    }

    if (status !== undefined && status !== key.status) {
      const oldStatus = key.status;
      key.status = status;
      key.accessLog.push(
        `${new Date().toLocaleString()}: Status updated from "${oldStatus}" to "${status}"`
      );
      isChanged = true;
      statusChanged = true;
    }

    if (isChanged) {
      await key.save();
      if (statusChanged) {
        sendKeyStatusEmail(key); // send email in background
      }
    }

    return res.json({ message: "Key updated successfully", key });
  } catch (error) {
    next(error);
  }
};
