import { Key } from "../../models";

export const getKeysData = async (req, res, next) => {
  try {
    // 1. Fetch keys from database
    const keys = await Key.find().sort({ createdAt: -1 });

    // 2. Fetch unique property codes currently active in the database for autocomplete suggestions
    const properties = await Key.distinct("communityCode");

    // 3. Fetch unique units currently active in the database
    const units = await Key.distinct("unit");

    return res.json({
      keys,
      properties,
      units,
    });
  } catch (error) {
    next(error);
  }
};
