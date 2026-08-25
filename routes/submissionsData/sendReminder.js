import { JotformSubmission } from "../../models/index.js";
import nodemailer from "nodemailer";

/*
// =========================================================================
// 🔴 PREVIOUS IMPLEMENTATION (COMMENTED OUT AS REQUESTED)
// =========================================================================
const getFieldValue = (answers, fieldName, keyName = "answer") => {
  const field = Object.values(answers || {}).find(
    (item) => item?.text === fieldName
  );
  if (!field) return null;
  const value = field?.[keyName] ?? null;
  if (typeof value === "string") return value.replace(/<[^>]*>/g, "").trim();
  return value;
};

export default async (req, res, next) => {
  try {
    const { submissionId } = req;
    if (!submissionId) {
      return res.status(400).json({ message: "Submission ID is required" });
    }

    const submission = await JotformSubmission.findOne({ submissionId });

    if (!submission) {
      return res.status(404).json({ message: "Submission not found" });
    }

    const answers = submission.answers || {};

    const recipient =
      getFieldValue(answers, "User Email") || submission.replyEmail;
    if (!recipient) {
      return res
        .status(400)
        .json({ message: "No email address found for this submission" });
    }

    const formName =
      submission.formName ||
      getFieldValue(answers, "Uploader Header") ||
      "Upload Form";
    const propertyName = getFieldValue(answers, "RM Short Name") || "-";
    const unitName = getFieldValue(answers, "RM Unit Name") || "-";
    const recipientName = getFieldValue(answers, "User Name") || "Resident";

    const formId = submission.formId;
    const reactAppUrl =
      process.env.REACT_APP_URL || "https://app.premiumpd.com";
    const uploadLink = `${reactAppUrl}/jotform/form/${formId}?submissionId=${submissionId}`;

    const subject = `Reminder: Pending Document Upload for ${formName}`;
    const payload = {
      formName,
      propertyName,
      unitName,
      uploadLink,
      recipientName,
    };

    const replyTo =
      getFieldValue(answers, "Reply Email") || "turnovers@premiumpd.com";

    await sendMail(
      subject,
      payload,
      recipient,
      "template-reminderFromSubmissionList",
      process.env.TEST_BCC_EMAIL || "",
      [],
      replyTo
    );

    console.log(
      `Manual Reminder email sent to ${recipient} for submission ${submissionId}`
    );
    return res.json({
      success: true,
      message: "Reminder email sent successfully",
    });
  } catch (error) {
    console.error("[submissionsData/sendReminder] Fatal error:", error);
    next(error);
  }
};
*/

// =========================================================================
// ✅ NEW IMPLEMENTATION (CUSTOM REMINDER MODAL)
// =========================================================================
const getFieldValue = (answers, fieldName, keyName = "answer") => {
  const field = Object.values(answers || {}).find(
    (item) => item?.text === fieldName
  );
  if (!field) return null;
  const value = field?.[keyName] ?? null;
  if (typeof value === "string") return value.replace(/<[^>]*>/g, "").trim();
  return value;
};

export default async (req, res, next) => {
  try {
    const submissionId = req.submissionId || req.params.submissionId;
    const { recipients, subject, content } = req.body;

    if (!submissionId) {
      return res.status(400).json({ message: "Submission ID is required" });
    }

    // Process & normalize recipient emails
    let recipientList = [];
    if (Array.isArray(recipients)) {
      recipientList = recipients.map((e) => String(e).trim()).filter(Boolean);
    } else if (typeof recipients === "string") {
      recipientList = recipients
        .split(/[,;\s]+/)
        .map((e) => e.trim())
        .filter(Boolean);
    }

    if (recipientList.length === 0) {
      return res
        .status(400)
        .json({ message: "At least one recipient email address is required" });
    }

    if (!subject || !String(subject).trim()) {
      return res.status(400).json({ message: "Subject is required" });
    }

    if (!content || !String(content).trim()) {
      return res.status(400).json({ message: "Content is required" });
    }

    const submission = await JotformSubmission.findOneAndUpdate(
      { submissionId },
      { lastManualReminderSentAt: new Date() },
      { new: true }
    );
    const answers = submission?.answers || {};
    const replyTo =
      getFieldValue(answers, "Reply Email") || "turnovers@premiumpd.com";

    // Generate upload link if formId is present
    const formId = submission?.formId;
    const reactAppUrl =
      process.env.REACT_APP_URL || "https://app.premiumpd.com";
    const uploadLink = formId
      ? `${reactAppUrl}/jotform/form/${formId}?submissionId=${submissionId}`
      : "";

    // Format HTML content matching template-reminderFromSubmissionList.ejs
    const htmlBody = `<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <title>${String(subject).trim()}</title>
  </head>
  <body
    style="
      font-family: Arial, sans-serif;
      line-height: 1.6;
      color: #333;
      max-width: 600px;
      margin: 0 auto;
      padding: 20px;
    "
  >
    <div style="text-align: center; margin-bottom: 24px">
      <h2 style="color: #1c3260; margin-bottom: 4px">
        Reminder - Your Photos & Videos Haven't Uploaded Yet.
      </h2>
    </div>

    <div>
      ${String(content).trim().replace(/\n/g, "<br/>")}

      ${
        uploadLink
          ? `
      <div style="text-align: center; margin-top: 32px; margin-bottom: 16px">
        <a
          href="${uploadLink}"
          style="
            background-color: #1c3260;
            color: #ffffff;
            padding: 12px 24px;
            text-decoration: none;
            border-radius: 4px;
            font-weight: bold;
            display: inline-block;
          "
        >
          Upload Files Now
        </a>
      </div>`
          : ""
      }
    </div>
  </body>
</html>`;

    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.GMAIL_EMAIL_USER?.trim(),
        pass: process.env.GMAIL_APP_PASSWORD?.trim(),
      },
    });

    const mailOptions = {
      from: `"Premium Properties" <${process.env.GMAIL_EMAIL_USER}>`,
      to: recipientList,
      bcc: process.env.TEST_BCC_EMAIL || "",
      subject: String(subject).trim(),
      html: htmlBody,
      replyTo: replyTo,
    };

    const result = await transporter.sendMail(mailOptions);
    console.log("Custom Reminder Email sent successfully:", result);
    transporter.close();

    return res.json({
      success: true,
      message: "Reminder email sent successfully",
    });
  } catch (error) {
    console.error("[submissionsData/sendReminder] Fatal error:", error);
    next(error);
  }
};
