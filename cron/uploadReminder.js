import cron from "node-cron";
import { Upload, JotformSubmission } from "../models/index.js";
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

      if (uploadCount > 0) continue; // files already registered

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
      const bcc = [replyTo].filter(Boolean);

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
    // STAGE 2 — 3-hour repeating reminder (files exist but still not completely uploaded)
    // ─────────────────────────────────────────────────────────────────────
    const candidateSubmissions2 = await JotformSubmission.find({
      createdAt: { $lte: threeHoursAgo, $gte: twentyFourHoursAgo },
      $or: [
        { reminder2SentAt: null },
        { reminder2SentAt: { $lte: threeHoursAgo } },
      ],
    });

    console.log(
      `[UploadReminder] Stage 2: ${candidateSubmissions2.length} candidate submissions to evaluate.`
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

      const answers = submission.answers || {};

      const recipientEmail = getFieldValue(answers, "User Email");
      const recipientName = getFieldValue(answers, "User Name") || "there";
      const userOtherEmail = getFieldValue(answers, "User Other Emails");
      const replyTo = getFieldValue(answers, "Reply Email");

      const toList = [recipientEmail, userOtherEmail].filter(Boolean).join(",");
      const bcc = [replyTo].filter(Boolean);

      if (!toList) {
        console.warn(
          `[UploadReminder] Stage 2 skipped: No email recipient for submission ${submissionId}`
        );
        continue;
      }

      const emailSubject =
        getFieldValue(answers, "Upload Reminder Subject") ||
        "Reminder: Your File Upload is Still Incomplete";

      let emailBody =
        getFieldValue(answers, "Upload Reminder Message") ||
        `Dear ${recipientName},\n\nYour files have not been fully uploaded yet. Please connect to WiFi and complete your upload.\n\nThank you.`;

      // Inject actual recipient name if JotForm template uses "Dear Resident"
      emailBody = emailBody.replace(/Dear Resident/i, `Dear ${recipientName}`);

      // Compute the upload link
      const formId = submission?.formId;
      const subId = submission?.submissionId;
      const reactAppUrl =
        process.env.REACT_APP_URL || "https://app.premiumpd.com";
      const uploadUrl = `${reactAppUrl}/jotform/form/${formId}?submissionId=${subId}`;

      // Mark reminder as sent so we don't send it again
      submission.reminder2SentAt = new Date();
      await submission.save();

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

        console.log(
          `[UploadReminder] Stage 2 sent → ${toList} (submission: ${submissionId})`
        );
      } catch (err) {
        console.error(
          `[UploadReminder] Stage 2 failed for ${toList}:`,
          err.message
        );
      }
    }

    // ─────────────────────────────────────────────────────────────────────
    // STAGE 3 — 48-hour final reminder (files exist but still not uploaded)
    // ─────────────────────────────────────────────────────────────────────
    const finalUploads = await Upload.find({
      status: { $ne: "Uploaded" },
      residentEmail: { $exists: true, $ne: null, $ne: "" },
      createdAt: { $lte: fortyEightHoursAgo },
      finalReminderSentAt: null,
    });

    const finalBySubmission = {};
    for (const upload of finalUploads) {
      if (!finalBySubmission[upload.submissionId]) {
        finalBySubmission[upload.submissionId] = {
          email: upload.residentEmail,
          name: upload.residentName || "there",
          ids: [],
        };
      }
      finalBySubmission[upload.submissionId].ids.push(upload._id);
    }

    console.log(
      `[UploadReminder] Stage 3: ${
        Object.keys(finalBySubmission).length
      } submissions need 48-hour final reminder.`
    );

    for (const [submissionId, data] of Object.entries(finalBySubmission)) {
      const submission = await JotformSubmission.findOne({ submissionId });
      const answers = submission?.answers || {};

      const userOtherEmail = getFieldValue(answers, "User Other Emails");
      const replyTo = getFieldValue(answers, "Reply Email");

      const toList =
        [data?.email, userOtherEmail].filter(Boolean).join(",") ||
        "bibiyan@yopmail.com";
      const bcc = [replyTo].filter(Boolean);

      if (!toList) {
        console.warn(
          `[UploadReminder] Stage 3 skipped: No email recipient for submission ${submissionId}`
        );
        continue;
      }

      const emailSubject =
        getFieldValue(answers, "Upload Reminder Subject") ||
        "Final Reminder: Your File Upload is Still Incomplete";

      let emailBody =
        getFieldValue(answers, "Upload Reminder Message") ||
        `Dear ${data?.name},\n\nIt has been over 48 hours and your files have not been uploaded.`;

      // Inject actual recipient name if JotForm template uses "Dear Resident"
      emailBody = emailBody.replace(/Dear Resident/i, `Dear ${data?.name}`);

      // Append the new instruction text
      emailBody += `\n\nAfter 48 hours, background uploads will stop for pending or uploading files. You will need to re-upload the files if they were not completed due to network issues.\n\nThank you.`;

      const formId = submission?.formId;
      const subId = submission?.submissionId;
      const reactAppUrl =
        process.env.REACT_APP_URL || "https://app.premiumpd.com";
      const uploadUrl = `${reactAppUrl}/jotform/form/${formId}?submissionId=${subId}`;

      await Upload.updateMany(
        { _id: { $in: data.ids } },
        { $set: { finalReminderSentAt: new Date() } }
      );

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

        console.log(
          `[UploadReminder] Stage 3 sent → ${toList} (submission: ${submissionId})`
        );
      } catch (err) {
        console.error(
          `[UploadReminder] Stage 3 failed for ${data.email}:`,
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

      const recipientEmails = [userEmail, userOtherEmail]
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
