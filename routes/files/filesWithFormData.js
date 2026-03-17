import { JotformSubmission, Upload } from "../../models";

const getFieldValue = (answers, fieldName, keyName = "text") => {
  const field = Object.values(answers || {}).find(
    (item) => item.text === fieldName
  );

  if (!field) return null;

  const value = field?.[keyName] || null;

  if (typeof value === "string") {
    return value.replace(/<[^>]*>/g, "").trim();
  }

  return value;
};

export default async (req, res, next) => {
  try {
    const { submissionId } = req;

    const [files, submission] = await Promise.all([
      Upload.find({ submissionId }),
      JotformSubmission.findOne({ submissionId }),
    ]);

    const answers = submission?.answers || {};

    const formData = {
      clientName: getFieldValue(answers, "User Name"),
      email: getFieldValue(answers, "Email"),
      submissionDate: getFieldValue(answers, "Submission Date"),
      fullAddress: getFieldValue(answers, "Full Address"),
      unitType: getFieldValue(answers, "Unit Type"),
      rMShortName: getFieldValue(answers, "RM Short Name"),
      rMUnitName: getFieldValue(answers, "RM Unit Name"),
      userPhoneNumber: getFieldValue(answers, "User Phone Number"),
      tenantEmails: getFieldValue(answers, "Tenant Emails"),
      forwardingURL: getFieldValue(answers, "Forwarding URL"),
      replyEmail: getFieldValue(answers, "Reply Email"),
      bedrooms: getFieldValue(answers, "Bedrooms"),
      bathrooms: getFieldValue(answers, "Bathrooms"),
    };

    return res.json({
      message: "Files fetched successfully",
      submissionId,
      files,
      formData,
    });
  } catch (error) {
    console.error(error);
    next(error);
  }
};
