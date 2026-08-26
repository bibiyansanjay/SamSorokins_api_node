import cron from "node-cron";
import { Upload, JotformSubmission } from "../models/index.js";

/**
 * Cron function to check uploads and auto-mark submissions as submitted.
 *
 * Logic:
 * 1. First get jotformsubmissions from jotformsubmissions table where isSubmited === false (or not true).
 * 2. Then check in Upload table if we have an upload with the same submissionId and createdAt older than 1 hr.
 * 3. If matched, update isSubmited = true in jotformsubmissions table.
 */
export const autoMarkSubmitted = async () => {
  console.log(
    "[AutoMarkSubmitted] Running auto-mark submitted check at",
    new Date().toISOString()
  );

  try {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);

    // 1. Get all JotformSubmissions where isSubmited is false or not true
    const unsubmittedSubmissions = await JotformSubmission.find({
      isSubmited: { $ne: true },
    }).select("submissionId");

    if (!unsubmittedSubmissions || unsubmittedSubmissions.length === 0) {
      console.log(
        "[AutoMarkSubmitted] No unsubmitted JotformSubmissions found."
      );
      return;
    }

    const unsubmittedIds = unsubmittedSubmissions
      .map((s) => s.submissionId)
      .filter(Boolean);

    // 2. Check in Upload table if any upload exists with same submissionId and createdAt older than 1 hour
    const submissionIdsToUpdate = await Upload.distinct("submissionId", {
      submissionId: { $in: unsubmittedIds },
      createdAt: { $lte: oneHourAgo },
    });

    if (!submissionIdsToUpdate || submissionIdsToUpdate.length === 0) {
      console.log(
        "[AutoMarkSubmitted] No uploads older than 1 hour found for unsubmitted submissions."
      );
      return;
    }

    console.log("Update isSubmited = true in jotformsubmissions table", {
      submissionIdsToUpdate,
    });
    // 3. Update isSubmited = true in jotformsubmissions table
    const result = await JotformSubmission.updateMany(
      {
        submissionId: { $in: submissionIdsToUpdate },
        isSubmited: { $ne: true },
      },
      {
        $set: { isSubmited: true },
      }
    );

    console.log(
      `[AutoMarkSubmitted] Evaluated ${unsubmittedIds.length} unsubmitted submission(s). Updated ${result.modifiedCount} JotformSubmission record(s) to isSubmited = true.`
    );
  } catch (error) {
    console.error(
      "[AutoMarkSubmitted] Error in autoMarkSubmitted cron job:",
      error
    );
  }
};

// Schedule job to run every 1 hour
cron.schedule(
  "0 * * * *",
  async () => {
    await autoMarkSubmitted();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

export default autoMarkSubmitted;
