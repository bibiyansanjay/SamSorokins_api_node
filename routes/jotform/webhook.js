import createRecord from "../../utils/createRecord";
import multer from "multer";
//import axios from "axios";

const upload = multer();

// Handler
const webhookHandler = async (req, res) => {
  const body = req.body;
  const submissionID = body.submissionID;
  //console.log("Received webhook body:", body);
  console.log("Submission ID:", submissionID);
  if (!submissionID) {
    console.warn("submissionID not found in webhook body");
    return res.status(400).json({ message: "Missing submissionID" });
  }

  const data = {
    submission_id: submissionID,
  };

  const result = await createRecord("user_submissions", data, {});
  if (result.affectedRows === 0) {
    return res.status(404).json({ message: "Form saving failed" });
  }
};

// Export middleware + handler as an array
export default [upload.none(), webhookHandler];

//get submission from jotform
/*   try {
    const response = await axios.get(
      `https://eu-api.jotform.com/submission/${submissionID}?apiKey=${process.env.JOTFORM_API_KEY}`
    );
    console.log("submission data", response.data);

    return res.json({
      message: "Webhook processed successfully",
      submissionData: response.data,
    });
  } catch (error) {
    console.error("Failed to fetch submission data:", error.message);
    return res.status(500).json({
      message: "Failed to fetch submission data",
      error: error.message,
    });
  } */
