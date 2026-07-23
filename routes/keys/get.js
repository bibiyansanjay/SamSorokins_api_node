import { Key } from "../../models";

export const getKeysData = async (req, res, next) => {
  try {
    // 1. Fetch keys from database
    const keys = await Key.find().sort({ createdAt: -1 });

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
