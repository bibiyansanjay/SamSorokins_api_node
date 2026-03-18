import { JotformSubmission, Upload } from "../../models";

const getFieldValue = (answers, fieldName, keyName = "answer") => {
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

    const rawDate = getFieldValue(answers, "Submission Date");
    // "2026-03-17 00:00:00"

    const day = rawDate?.day;
    const month = rawDate?.month;
    const year = rawDate?.year;

    const submissionDate =
      day && month && year ? `${day}-${month}-${year}` : "-";

    const formData = {
      Name: getFieldValue(answers, "User Name"),
      Email: getFieldValue(answers, "User Email"),
      Submission_Date: submissionDate,
      Address: getFieldValue(answers, "Full Address"),
      Unit_Type: getFieldValue(answers, "Unit Type"),
      RM_Short_Name: getFieldValue(answers, "RM Short Name"),
      RM_Unit_Name: getFieldValue(answers, "RM Unit Name"),
      Phone_Number: getFieldValue(answers, "User Phone Number"),
      Other_Emails: getFieldValue(answers, "Tenant Emails"),
      Forwarding_URL: getFieldValue(answers, "Forwarding URL"),
      //   Reply_Email: getFieldValue(answers, "Reply Email"),
      Bedrooms: getFieldValue(answers, "Bedrooms"),
      Bathrooms: getFieldValue(answers, "Bathrooms"),
    };

    return res.json({
      message: "Files fetched successfully",
      submissionId,
      files,
      formData,
      answers,
    });
  } catch (error) {
    console.error(error);
    next(error);
  }
};
