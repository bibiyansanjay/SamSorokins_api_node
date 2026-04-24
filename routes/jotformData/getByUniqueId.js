import JotformSubmission from "../../models/jotformSubmission";

/**
 * @name /jotformData/uniqueId/:uniqueId GET
 * @memberof module:Routes.jotformData
 * @description Function to get form data from jotform db by uniqueId
 */
export default async (req, res, next) => {
  try {
    const { uniqueId } = req;

    if (!uniqueId) {
      return res.status(400).json({ message: "uniqueId is required!" });
    }

    const data = await JotformSubmission.findOne({ uniqueId });

    if (!data) {
      return res.status(404).json({ message: "Submission not found!" });
    }

    return res.json({
      success: true,
      data: { submissionId: data.submissionId, formId: data.formId },
    });
  } catch (error) {
    next(error);
  }
};
