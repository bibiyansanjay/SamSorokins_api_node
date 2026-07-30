import { Key } from "../../models";

export const getKeysData = async (req, res, next) => {
  try {
    // 1. Fetch active keys (excluding soft-deleted)
    const keys = await Key.find({ isDeleted: { $ne: true } }).sort({
      createdAt: -1,
    });

    // 2. Fetch unique properties currently active in the database for autocomplete suggestions
    const properties = await Key.distinct("property");

    return res.json({
      keys,
      properties,
    });
  } catch (error) {
    next(error);
  }
};
