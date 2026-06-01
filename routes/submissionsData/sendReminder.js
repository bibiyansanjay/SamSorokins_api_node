import { JotformSubmission } from "../../models/index.js";
import sendMail from "../../methods/sendMail.js";

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
    // console.log({ submissionId });
    if (!submissionId) {
      return res.status(400).json({ message: "Submission ID is required" });
    }

    const submission = await JotformSubmission.findOne({ submissionId });

    if (!submission) {
      return res.status(404).json({ message: "Submission not found" });
    }

    const answers = submission.answers || {};

    // console.log(
    //   'getFieldValue(answers, "User Email")',
    //   getFieldValue(answers, "User Email")
    // );
    // console.log("submission.replyEmail", submission.replyEmail);

    const recipient =
      getFieldValue(answers, "User Email") || submission.replyEmail;
    // const recipient = "testrohit1993@gmail.com";
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

    // const replyTo = "testrohit1993@gmail.com";
    // console.log(
    //   ' getFieldValue(answers, "Reply Email")',
    //   getFieldValue(answers, "Reply Email")
    // );

    // Send the email with template template-reminderFromSubmissionList
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
