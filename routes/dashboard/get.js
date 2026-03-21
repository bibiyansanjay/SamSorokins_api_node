import { JotformSubmission, Upload } from "../../models/index.js";

export default async (req, res, next) => {
  try {
    // Determine unique form IDs to count distinct forms
    const distinctForms = await JotformSubmission.distinct("formId");
    const formsCount = distinctForms.length;

    const [submissionsCount, filesCount] = await Promise.all([
      JotformSubmission.countDocuments(),
      Upload.countDocuments(),
    ]);
    
    return res.json({
      message: "Dashboard stats fetched successfully",
      stats: {
        forms: formsCount,
        submissions: submissionsCount,
        files: filesCount,
      }
    });
  } catch (error) {
    console.error("[dashboard/get]", error);
    next(error);
  }
};

