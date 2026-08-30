import { JotformSubmission } from "../../models/index.js";
import { clearDuplicatesCache } from "./getDuplicates.js";
import { clearResolvedDuplicatesCache } from "./getResolvedDuplicates.js";

export default async (req, res, next) => {
  try {
    const { submissionIds } = req.body;

    if (!Array.isArray(submissionIds) || submissionIds.length === 0) {
      return res
        .status(400)
        .json({ success: false, message: "submissionIds array is required" });
    }

    // Mark all provided submissionIds as resolved in DB
    const result = await JotformSubmission.updateMany(
      { submissionId: { $in: submissionIds } },
      { $set: { isDuplicateResolved: true } }
    );

    // Invalidate duplicate caches
    clearDuplicatesCache();
    clearResolvedDuplicatesCache();

    return res.json({
      success: true,
      message: `Marked ${result.modifiedCount || submissionIds.length} duplicate submissions as resolved.`,
      modifiedCount: result.modifiedCount,
    });
  } catch (error) {
    console.error("[resolveDuplicates] Error:", error);
    next(error);
  }
};
