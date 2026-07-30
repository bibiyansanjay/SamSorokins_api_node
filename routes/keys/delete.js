import { Key } from "../../models";

export default async (req, res, next) => {
  try {
    const { id } = req.params;

    const keyItem = await Key.findById(id);
    if (!keyItem) {
      return res.status(404).json({ message: "Key request not found." });
    }

    keyItem.isDeleted = true;
    keyItem.deletedAt = new Date();
    const logMessage = `${new Date().toLocaleString()}: Key request soft-deleted.`;
    keyItem.accessLog.push(logMessage);

    await keyItem.save();

    return res.json({
      success: true,
      message: "Key request deleted successfully.",
      key: keyItem,
    });
  } catch (error) {
    next(error);
  }
};
