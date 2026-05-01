import JotformSubmission from "../../models/jotformSubmission";

export default async (req, res, next) => {
  try {
    const { submissionId } = req;

    if (!submissionId) {
      return res.status(400).json({ message: "Submission Id is required!" });
    }

    const submission = await JotformSubmission.findOneAndUpdate(
      { submissionId: submissionId },
      { $set: { isSubmited: true } },
      { new: true }
    );

    if (!submission) {
      return res.status(404).json({ message: "Submission not found" });
    }

    return res.json({
      success: true,
      message: "Submission marked as submitted successfully",
      data: submission,
    });
  } catch (error) {
    next(error);
  }
};
