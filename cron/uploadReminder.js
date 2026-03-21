import cron from "node-cron";
import { Upload, JotformSubmission } from "../models/index.js";
import sendMail from "../methods/sendMail.js";

/**
 * Helper: extract a field value from JotForm answers by the field's `text` label.
 */
const getFieldValue = (answers, fieldName, keyName = "answer") => {
  const field = Object.values(answers || {}).find(
    (item) => item.text === fieldName
  );
  if (!field) return null;
  const value = field?.[keyName] ?? null;
  if (typeof value === "string") return value.replace(/<[^>]*>/g, "").trim();
  return value;
};

/**
 * TWO-STAGE UPLOAD REMINDER CRON
 *
 * Stage 1 — 1 hour after submission:
 *   Condition : Submission has "Files to Upload" with at least one answer
 *               AND no Upload documents exist yet for that submissionId.
 *   Email to  : answers["Email"]
 *   Subject   : answers["Submission Reminder Subject"]
 *   Body      : answers["Submission Reminder Message"]
 *   Tracked by: JotformSubmission.reminder1SentAt
 *
 * Stage 2 — 24 hours after files were registered but none uploaded:
 *   Condition : Upload documents exist but none have status "Uploaded".
 *   Email to  : upload.residentEmail
 *   Subject   : answers["Upload Reminder Subject"]  (from the submission)
 *   Body      : answers["Upload Reminder Message"]  (from the submission)
 *   Tracked by: Upload.reminder2SentAt
 */
const sendUploadReminders = async () => {
  console.log(
    "[UploadReminder] Running two-stage reminder check at",
    new Date().toISOString()
  );

  const now = Date.now();
  const oneHourAgo = new Date(now - 1 * 60 * 60 * 1000);
  const twentyFourHoursAgo = new Date(now - 24 * 60 * 60 * 1000);

  try {
    // ─────────────────────────────────────────────────────────────────────
    // STAGE 1 — 1-hour reminder (no files uploaded at all yet)
    // ─────────────────────────────────────────────────────────────────────
    const candidateSubmissions = await JotformSubmission.find({
      reminder1SentAt: null,
      createdAt: { $lte: oneHourAgo },
    });

    console.log(
      `[UploadReminder] Stage 1: ${candidateSubmissions.length} candidate submissions to evaluate.`
    );

    for (const submission of candidateSubmissions) {
      const answers = submission.answers || {};

      // Check "Files to Upload" checkbox has at least one value
      const filesToUploadField = Object.values(answers).find(
        (item) => item?.text === "Files to Upload"
      );
      const requiredUploads = filesToUploadField?.answer;
      const hasRequiredUploads =
        Array.isArray(requiredUploads) && requiredUploads.length > 0;

      if (!hasRequiredUploads) continue;

      // Check if any Upload records exist for this submission
      const uploadCount = await Upload.countDocuments({
        submissionId: submission.submissionId,
      });

      if (uploadCount > 0) continue; // files already registered

      // Pull all dynamic content from the submission answers
      const recipientEmail = getFieldValue(answers, "Email");
      const recipientName = getFieldValue(answers, "User Name") || "there";

      const emailSubject =
        getFieldValue(answers, "Submission Reminder Subject") ||
        "Reminder: Please Upload Your Files";

      let emailBody =
        getFieldValue(answers, "Submission Reminder Message") ||
        `Dear ${recipientName},\n\nWe noticed you haven't uploaded your files yet. Please do so as soon as possible.\n\nThank you.`;

      // Inject actual recipient name if JotForm template uses "Dear Resident"
      emailBody = emailBody.replace(/Dear Resident/i, `Dear ${recipientName}`);

      // Always mark reminder1SentAt so we don't re-evaluate every poll
      submission.reminder1SentAt = new Date();
      await submission.save();

      if (!recipientEmail) {
        console.warn(
          `[UploadReminder] Stage 1: No email for submission ${submission.submissionId} — skipped.`
        );
        continue;
      }

      try {
        await sendMail(
          emailSubject,
          { subject: emailSubject, body: emailBody },
          recipientEmail,
          "template-uploadReminder",
          "",
          []
        );

        console.log(
          `[UploadReminder] Stage 1 sent → ${recipientEmail} (submission: ${submission.submissionId})`
        );
      } catch (err) {
        console.error(
          `[UploadReminder] Stage 1 failed for ${recipientEmail}:`,
          err.message
        );
      }
    }

    // ─────────────────────────────────────────────────────────────────────
    // STAGE 2 — 24-hour reminder (files exist but still not uploaded)
    // ─────────────────────────────────────────────────────────────────────
    const staleUploads = await Upload.find({
      status: { $ne: "Uploaded" },
      reminder2SentAt: null,
      residentEmail: { $exists: true, $ne: null, $ne: "" },
      createdAt: { $lte: twentyFourHoursAgo },
    });

    // Group by submissionId → one email per resident
    const bySubmission = {};
    for (const upload of staleUploads) {
      if (!bySubmission[upload.submissionId]) {
        bySubmission[upload.submissionId] = {
          email: upload.residentEmail,
          name: upload.residentName || "there",
          ids: [],
        };
      }
      bySubmission[upload.submissionId].ids.push(upload._id);
    }

    console.log(
      `[UploadReminder] Stage 2: ${
        Object.keys(bySubmission).length
      } submissions need 24hr reminder.`
    );

    for (const [submissionId, data] of Object.entries(bySubmission)) {
      // Pull dynamic subject/body from JotForm submission
      const submission = await JotformSubmission.findOne({ submissionId });
      const answers = submission?.answers || {};

      const emailSubject =
        getFieldValue(answers, "Upload Reminder Subject") ||
        "Final Reminder: Your File Upload is Still Incomplete";
      let emailBody =
        getFieldValue(answers, "Upload Reminder Message") ||
        `Dear ${data.name},\n\nIt has been over 24 hours and your files have not been uploaded. Please connect to WiFi and complete your upload.\n\nThank you.`;

      // Inject actual recipient name if JotForm template uses "Dear Resident"
      emailBody = emailBody.replace(/Dear Resident/i, `Dear ${data.name}`);

      // Mark all uploads for this submission as reminder2 sent (before email so failures don't repeat)
      await Upload.updateMany(
        { _id: { $in: data.ids } },
        { $set: { reminder2SentAt: new Date() } }
      );

      try {
        await sendMail(
          emailSubject,
          { subject: emailSubject, body: emailBody },
          data.email,
          "template-uploadReminder",
          "",
          []
        );

        console.log(
          `[UploadReminder] Stage 2 sent → ${data.email} (submission: ${submissionId})`
        );
      } catch (err) {
        console.error(
          `[UploadReminder] Stage 2 failed for ${data.email}:`,
          err.message
        );
      }
    }

    console.log("[UploadReminder] Two-stage check complete.");
  } catch (error) {
    console.error("[UploadReminder] Fatal error in reminder cron job:", error);
  }
};

// Run every 30 minutes — catches both the 1hr and 24hr windows accurately
cron.schedule(
  "*/30 * * * *",
  async () => {
    console.log(
      "[UploadReminder] Cron triggered at:",
      new Date().toISOString()
    );
    await sendUploadReminders();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

export default sendUploadReminders;
