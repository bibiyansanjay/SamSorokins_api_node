import axios from "axios";
import JotformSubmission from "../../models/jotformSubmission";

const apiKey =
  process.env.JOTFORM_API_KEY || "da724b69ac2c6dc23adf791b768e8674";

/**
 * @name /jotform GET
 * @memberof module:Routes.jotform
 * @description Function to get submited form data from jot form by submission id
 */

function getAnswerByName(answers, fieldName, keyName = "answer") {
  if (!answers) return null;
  const field = Object.values(answers).find((item) => item.text === fieldName);
  if (!field) return null;

  const value = field?.[keyName] || null;

  if (typeof value === "string") {
    return value.replace(/<[^>]*>/g, "").trim();
  }

  return value;
}

function getUniqueId(answers, fieldName, keyName = "answer") {
  if (!answers) return null;

  const field = Object.values(answers).find((item) => item.name === fieldName);

  if (!field) return "";

  const value = field?.[keyName] || "";

  if (typeof value === "string") {
    return value.replace(/<[^>]*>/g, "").trim();
  }

  return value;
}

export default async (req, res, next) => {
  try {
    const { submissionId } = req;

    if (!submissionId) {
      return res.status(400).json({ message: "Submission Id is required!" });
    }

    const data = await JotformSubmission.findOne({
      submissionId: submissionId,
    });

    // Safely find the "Files to Upload" answer field
    const fileUploadAnswer = data?.answers
      ? Object.values(data.answers).find(
          (item) => item.text === "Files to Upload"
        )
      : null;

    // Check if the answer value is empty/missing
    const isEmpty =
      !fileUploadAnswer?.answer || fileUploadAnswer.answer.length === 0;

    if (data && isEmpty) {
      return res.json({
        success: true,
        formKeys: {
          requiredUploads: "nothing_to_upload",
        },
      });
    }
    // Call JotForm API
    const response = await axios.get(
      `https://premiumpd.jotform.com/API/submission/${submissionId}`,
      {
        params: { apiKey },
      }
    );

    const jotformData = response?.data;
    const content = jotformData?.content;
    const answers = jotformData?.content?.answers;

    const uniqueIdURL = getUniqueId(answers, "uniqueId");

    let uniqueId = "";
    if (uniqueIdURL) {
      try {
        uniqueId = new URL(uniqueIdURL).pathname
          .split("/")
          .filter(Boolean)
          .pop();
      } catch (err) {
        uniqueId = String(uniqueIdURL).split("/").filter(Boolean).pop() || "";
      }
    }

    const replyEmail =
      getAnswerByName(answers, "Reply Email") ||
      getAnswerByName(answers, "Email") ||
      "";
    const formName = getAnswerByName(answers, "Uploader Header") || "";

    // Save into MongoDB

    const submission = await JotformSubmission.findOneAndUpdate(
      { submissionId: content.id },
      {
        submissionId: content.id,
        formId: content.form_id,
        ip: content.ip,
        status: content.status,
        createdAt: content.created_at,
        updatedAt: content.updated_at,
        answers: content.answers,
        uniqueId: uniqueId || "",
        replyEmail: replyEmail,
        formName: formName,

        // raw: jotformData,
      },
      {
        upsert: true,
        new: true,
      }
    );

    const residentName = getAnswerByName(answers, "User Name");
    const email = getAnswerByName(answers, "User Email");

    // const formTitle = getAnswerByName(answers, "fileUploader"); //File Uploader Tool (Form Title) //text
    const instTitle = getAnswerByName(answers, "Uploader Header"); //Move In Condition Report File Uploader (Instruction Title) //text
    const generalInstructions = getAnswerByName(
      answers,
      "General Instructions"
    ); // Thanks for answering all the the questions //text

    const videoUploadTitle = getAnswerByName(answers, "videoUpload382"); //Video Upload Instructions(Title) //text
    const videoUploadInstruction = getAnswerByName(
      answers,
      "Video Upload Instructions"
    ); //Please upload the video you took...(Instruction) //text
    const photoUploadTitle = getAnswerByName(answers, "photoUpload384"); //Photo Upload Instructions(Title) //text
    const photoUploadInstruction = getAnswerByName(
      answers,
      "Photo Upload Instructions"
    ); //Please upload the photos you took of...(Instruction)//text

    const fileUploadInstruction = getAnswerByName(
      answers,
      "File Upload Instructions"
    );

    const finalInstructions = getAnswerByName(answers, "Final Instructions"); //Thanks for selecting your files... //text

    const requiredUploads = getAnswerByName(answers, "Files to Upload"); //Thanks for selecting your files... //text
    const forwarding_URL = getAnswerByName(answers, "Forwarding URL");

    const unitName = getAnswerByName(answers, "RM Unit Name");
    const propertyName = getAnswerByName(answers, "RM Short Name");

    // finalInstructions,
    //   generalInstructions,
    //   photoUploadInstruction,
    //   requiredUploads;
    return res.json({
      success: true,
      formKeys: {
        // formTitle,
        instTitle,
        generalInstructions,
        videoUploadTitle,
        videoUploadInstruction,
        photoUploadInstruction,
        photoUploadTitle,
        fileUploadInstruction,
        finalInstructions,
        requiredUploads,
        forwarding_URL,
        unitName,
        propertyName,
        formName,
      },
      userData: {
        residentName,
        email,
      },
      formData: submission,
    });
  } catch (error) {
    next(error);
  }
};
