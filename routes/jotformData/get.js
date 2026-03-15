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
  const field = Object.values(answers).find((item) => item.name === fieldName);
  if (!field) return null;

  const value = field?.[keyName] || null;

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

    // Call JotForm API
    const response = await axios.get(
      `https://premiumpd.jotform.com/API/submission/${submissionId}`,
      {
        params: { apiKey },
      }
    );

    const jotformData = response?.data;
    const content = jotformData?.content;

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
        // raw: jotformData,
      },
      {
        upsert: true,
        new: true,
      }
    );

    const answers = jotformData?.content?.answers;

    const residentName = getAnswerByName(answers, "Resident_Name");
    const email = getAnswerByName(answers, "email");

    const formTitle = getAnswerByName(answers, "fileUploader", "text"); //File Uploader Tool (Form Title) //text
    const instTitle = getAnswerByName(answers, "moveIn380", "text"); //Move In Condition Report File Uploader (Instruction Title) //text
    const generalInstructions = getAnswerByName(answers, "generalInstructions"); // Thanks for answering all the the questions //answer

    const videoUploadTitle = getAnswerByName(answers, "videoUpload382", "text"); //Video Upload Instructions(Title) //text
    const videoUploadInstruction = getAnswerByName(answers, "input385", "text"); //Please upload the video you took...(Instruction) //text
    const photoUploadInstruction = getAnswerByName(answers, "photoUpload"); //Please upload the photos you took of...(Instruction)//answer
    const photoUploadTitle = getAnswerByName(answers, "photoUpload384", "text"); //Photo Upload Instructions(Title) //text
    const finalInstructions = getAnswerByName(answers, "finalInstructions"); //Thanks for selecting your files... //answer

    return res.json({
      success: true,
      formKeys: {
        formTitle,
        instTitle,
        generalInstructions,
        videoUploadTitle,
        videoUploadInstruction,
        photoUploadInstruction,
        photoUploadTitle,
        finalInstructions,
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
