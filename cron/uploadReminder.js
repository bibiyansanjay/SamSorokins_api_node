import cron from "node-cron";
import {
  Upload,
  JotformSubmission,
  UploadReminderLog,
} from "../models/index.js";
import sendMail from "../methods/sendMail.js";

/**
 * Helper: extract a field value from JotForm answers by the field's `text` label.
 */
const getFieldValue = (answers, fieldName, keyName = "answer") => {
  const field = Object.values(answers || {}).find(
    (item) => item?.text === fieldName
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
const samEmail = process.env.SAM_EMAIL;
const jesicaEmail = process.env.JESSICA_EMAIL;
const testBccEmail = process.env.TEST_BCC_EMAIL || "testrohit1993@gmail.com";

const sendUploadReminders = async () => {
  console.log(
    "[UploadReminder] Running multi-stage reminder check at",
    new Date().toISOString()
  );

  const now = Date.now();
  const oneHourAgo = new Date(now - 1 * 60 * 60 * 1000);
  const threeHoursAgo = new Date(now - 3 * 60 * 60 * 1000);
  const twentyFourHoursAgo = new Date(now - 24 * 60 * 60 * 1000);
  const fortyEightHoursAgo = new Date(now - 48 * 60 * 60 * 1000);

  // const bcc = [samEmail, jesicaEmail].filter(Boolean);
  try {
    // ─────────────────────────────────────────────────────────────────────
    // STAGE 1 — 1-hour reminder (no files uploaded at all yet)
    // ─────────────────────────────────────────────────────────────────────
    const candidateSubmissions = await JotformSubmission.find({
      reminder1SentAt: null,
      createdAt: { $lte: oneHourAgo, $gte: twentyFourHoursAgo },
    });

    console.log(
      `[UploadReminder] Stage 1: ${candidateSubmissions.length} candidate submissions to evaluate.`
    );

    // console.log(candidateSubmissions);
    for (const submission of candidateSubmissions) {
      const answers = submission.answers || {};

      // Check "Files to Upload" checkbox has at least one value
      const filesToUploadField = Object.values(answers).find(
        (item) => item?.text === "Files to Upload"
      );
      // console.log({ filesToUploadField }, submission.submissionId);
      const requiredUploads = filesToUploadField?.answer;
      const hasRequiredUploads =
        Array.isArray(requiredUploads) && requiredUploads.length > 0;
      // console.log({ hasRequiredUploads }, submission.submissionId);

      if (!hasRequiredUploads) continue;

      // Check if any Upload records exist for this submission
      const uploadCount = await Upload.countDocuments({
        submissionId: submission.submissionId,
      });

      if (uploadCount > 0 && submission?.isSubmited === true) continue; // files already registered

      // Pull all dynamic content from the submission answers
      const recipientEmail = getFieldValue(answers, "User Email");
      const recipientName = getFieldValue(answers, "User Name") || "there";

      // const tenantEmail = getFieldValue(answers, "Tenant Emails");

      const userOtherEmail = getFieldValue(answers, "User Other Emails");

      const replyTo = getFieldValue(answers, "Reply Email");
      // const replyTo = "tech.rohitchabra@gmail.com";

      console.log(
        { recipientName },
        { recipientEmail },
        { userOtherEmail },
        { replyTo },
        submission.submissionId
      );
      // return null;
      const emailSubject =
        getFieldValue(answers, "Submission Reminder Subject") ||
        "Reminder: Please Upload Your Files";

      let emailBody =
        getFieldValue(answers, "Submission Reminder Message") ||
        `Dear ${recipientName},\n\nWe noticed you haven't uploaded your files yet. Please do so as soon as possible.\n\nThank you.`;

      // Inject actual recipient name if JotForm template uses "Dear Resident"
      emailBody = emailBody.replace(/Dear Resident/i, `Dear ${recipientName}`);

      // Compute the upload link
      const formId = submission?.formId;
      const subId = submission?.submissionId;
      const reactAppUrl =
        process.env.REACT_APP_URL || "https://app.premiumpd.com";
      const uploadUrl = `${reactAppUrl}/jotform/form/${formId}?submissionId=${subId}`;

      // Always mark reminder1SentAt so we don't re-evaluate every poll
      submission.reminder1SentAt = new Date();
      await submission.save();

      if (!recipientEmail) {
        console.warn(
          `[UploadReminder] Stage 1: No email for submission ${submission.submissionId} — skipped.`
        );
        continue;
      }

      const toList = [recipientEmail, userOtherEmail].filter(Boolean).join(",");
      const bcc = [replyTo, testBccEmail].filter(Boolean);

      try {
        await sendMail(
          emailSubject,
          { subject: emailSubject, body: emailBody, uploadUrl },
          toList,
          "template-uploadReminder",
          "",
          [],
          replyTo,
          bcc
        );

        await UploadReminderLog.create({
          email: toList,
          submissionId: submission.submissionId,
          formId: submission.formId,
          reminderType: "1hr Reminder",
        });

        console.log(
          `[UploadReminder] Stage 1 sent → ${toList} (submission: ${submission.submissionId})`
        );
      } catch (err) {
        console.error(
          `[UploadReminder] Stage 1 failed for ${recipientEmail}:`,
          err.message
        );
      }
    }

    // ─────────────────────────────────────────────────────────────────────
    // STAGE 2 — Repeating Reminders (3h, 6h, 12h, 24h, 36h final)
    // ─────────────────────────────────────────────────────────────────────
    const candidateSubmissions2 = await JotformSubmission.find({
      createdAt: { $lte: threeHoursAgo },
    });

    console.log(
      `[UploadReminder] Stage 2: ${candidateSubmissions2.length} candidate submissions to evaluate for multi-stage reminders.`
    );

    for (const submission of candidateSubmissions2) {
      const { submissionId } = submission;

      // 1. Must have at least one file registered
      const totalUploads = await Upload.countDocuments({ submissionId });
      if (totalUploads === 0) continue;

      // 2. Must have at least one file NOT uploaded
      const pendingUploads = await Upload.countDocuments({
        submissionId,
        status: { $ne: "Uploaded" },
      });

      if (pendingUploads === 0) continue; // All files uploaded, Stage 4 handles success email

      const hoursSinceCreation = Math.floor(
        (now - new Date(submission.createdAt).getTime()) / (1000 * 60 * 60)
      );

      let stageToTrigger = 0;
      if (hoursSinceCreation >= 36) stageToTrigger = 5;
      else if (hoursSinceCreation >= 24) stageToTrigger = 4;
      else if (hoursSinceCreation >= 12) stageToTrigger = 3;
      else if (hoursSinceCreation >= 6) stageToTrigger = 2;
      else if (hoursSinceCreation >= 3) stageToTrigger = 1;

      // Only proceed if we reached a new stage milestone
      const currentStage = submission.uploadRemindersStage || 0;
      if (stageToTrigger <= currentStage) {
        continue;
      }

      const answers = submission.answers || {};

      const recipientEmail = getFieldValue(answers, "User Email");
      const recipientName = getFieldValue(answers, "User Name") || "there";
      const userOtherEmail = getFieldValue(answers, "User Other Emails");
      const replyTo = getFieldValue(answers, "Reply Email");

      const testBccEmail =
        process.env.TEST_BCC_EMAIL || "testrohit1993@gmail.com";

      const toList =
        [recipientEmail, userOtherEmail].filter(Boolean).join(",") ||
        testBccEmail;
      const bcc = [replyTo, testBccEmail].filter(Boolean);

      let emailSubject =
        getFieldValue(answers, "Upload Reminder Subject") ||
        "Reminder: Your File Upload is Still Incomplete";

      let emailBody =
        getFieldValue(answers, "Upload Reminder Message") ||
        `Dear ${recipientName},\n\nYour files have not been fully uploaded yet. Please connect to WiFi and complete your upload.\n\nThank you.`;

      // Inject actual recipient name if JotForm template uses "Dear Resident"
      emailBody = emailBody.replace(/Dear Resident/i, `Dear ${recipientName}`);

      // Final reminder verbiage
      if (stageToTrigger === 5) {
        emailSubject = "Final Reminder: Your File Upload is Still Incomplete";
        emailBody += `\n\nAfter 48 hours, background uploads will stop for pending or uploading files. You will need to re-upload the files if they were not completed due to network issues.\n\nThank you.`;
      }

      // Compute the upload link
      const formId = submission?.formId;
      const subId = submission?.submissionId;
      const reactAppUrl =
        process.env.REACT_APP_URL || "https://app.premiumpd.com";
      const uploadUrl = `${reactAppUrl}/jotform/form/${formId}?submissionId=${subId}`;

      // Mark the stage
      submission.uploadRemindersStage = stageToTrigger;
      await submission.save();

      let stageName = "";
      if (stageToTrigger === 1) stageName = "3hr Reminder";
      else if (stageToTrigger === 2) stageName = "6hr Reminder";
      else if (stageToTrigger === 3) stageName = "12hr Reminder";
      else if (stageToTrigger === 4) stageName = "24hr Reminder";
      else if (stageToTrigger === 5) stageName = "36hr Final Reminder";

      try {
        await sendMail(
          emailSubject,
          { subject: emailSubject, body: emailBody, uploadUrl },
          toList,
          "template-uploadReminder",
          "",
          [],
          replyTo,
          bcc
        );

        await UploadReminderLog.create({
          email: toList,
          submissionId: submissionId,
          formId: formId,
          reminderType: stageName,
        });

        console.log(
          `[UploadReminder] Stage 2 (Milestone ${stageToTrigger}) sent → ${toList} (submission: ${submissionId})`
        );
      } catch (err) {
        console.error(
          `[UploadReminder] Stage 2 (Milestone ${stageToTrigger}) failed for ${toList}:`,
          err.message
        );
      }
    }

    // ─────────────────────────────────────────────────────────────────────
    // STAGE 4 — Success Email (all files uploaded successfully)
    // ─────────────────────────────────────────────────────────────────────

    // 1. Get all submission IDs where EVERY upload record is "Uploaded"
    const completedUploadStats = await Upload.aggregate([
      {
        $group: {
          _id: "$submissionId",
          totalUploads: { $sum: 1 },
          completedUploads: {
            $sum: { $cond: [{ $eq: ["$status", "Uploaded"] }, 1, 0] },
          },
        },
      },
      {
        $match: {
          // totalUploads must equal completedUploads (meaning NO pending/failed files)
          $expr: { $eq: ["$totalUploads", "$completedUploads"] },
          // Must have at least one upload record
          totalUploads: { $gt: 0 },
        },
      },
    ]);

    const fullyUploadedSubIds = completedUploadStats.map(
      (stat) => stat.submissionId
    );

    // 2. Find those submissions that haven't received the email yet
    const pendingSuccessSubmissions = await JotformSubmission.find({
      submissionId: { $in: fullyUploadedSubIds },
      successFullUploadReminder: false,
    });

    console.log(
      `[UploadReminder] Stage 4: ${pendingSuccessSubmissions.length} fully completed submissions to notify.`
    );

    for (const submission of pendingSuccessSubmissions) {
      const { submissionId } = submission;

      // 3. Mark as sent and send email
      submission.successFullUploadReminder = true;
      await submission.save();

      const answers = submission.answers || {};
      const userEmail = getFieldValue(answers, "User Email");
      const userOtherEmail = getFieldValue(answers, "User Other Emails");

      const recipientEmails = [userEmail, userOtherEmail, testBccEmail]
        .map((e) => e?.trim())
        .filter(Boolean)
        .join(",");

      if (recipientEmails) {
        const totalUploads = await Upload.countDocuments({ submissionId });
        const residentName = getFieldValue(answers, "User Name") || "Resident";
        const formName = submission.formName || "Upload Form";

        try {
          await sendMail(
            `Uploads Complete - ${formName}`,
            {
              residentName,
              submissionId,
              formName,
              totalFiles: totalUploads,
              dateStr: new Date().toLocaleDateString(),
            },
            recipientEmails,
            "uploadSuccess",
            "",
            []
          );
          console.log(
            `[UploadReminder] Stage 4 sent → ${recipientEmails} (submission: ${submissionId})`
          );
        } catch (err) {
          console.error(
            `[UploadReminder] Stage 4 failed for ${submissionId}:`,
            err.message
          );
        }
      }
    }

    console.log("[UploadReminder] Multi-stage check complete.");
  } catch (error) {
    console.error("[UploadReminder] Fatal error in reminder cron job:", error);
  }
};

// Run every 30 minutes — catches both the 1hr and 24hr windows accurately
cron.schedule(
  "*/30 * * * *",
  // "*/2 * * * *", //every 2 mins for testing
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
