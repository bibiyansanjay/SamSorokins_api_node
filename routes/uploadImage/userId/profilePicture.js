import multer from "multer";
import awsFileUpload from "../../../utils/uploadImage";
import updateRecord from "../../../utils/updateRecord";
require("dotenv").config();

const storage = multer.memoryStorage();
const upload = multer({ storage });

export const config = {
  api: {
    bodyParser: false, // 👈 important to let multer handle multipart/form-data
  },
};

export default [
  upload.single("document"), // 👈 match key used in FormData
  async (req, res) => {
    try {
      const userId = req.user.id;
      const file = req.file; // multer attaches the file here

      if (!file) {
        return res.status(400).json({ message: "No file uploaded" });
      }

      const aws_bucket = process.env.AWS_BUCKET;

      const result = await awsFileUpload(
        aws_bucket, // ✅ only the bucket name
        `user/profilepicture/${Date.now()}`, // ✅ unique key
        file.buffer,
        file.mimetype
      );
      //console.log("result", result);
      // ✅ Check if result.location exists and is non-empty
      if (!result || !result.Location || result.Location.trim() === "") {
        console.error("Invalid S3 response or missing file location");
        return res.status(500).json({
          success: false,
          message: "Image uploaded to S3 but no URL returned",
        });
      }
      // 1. Perform the update
      const data = { profileImg: result.Location };
      const resultUpdate = await updateRecord("user", data, { id: userId });
      if (resultUpdate.affectedRows === 0) {
        console.log("user image not saved in db");
        return res.status(404).json({ message: "Internal Server Error" });
      }

      return res.status(200).json({
        success: true,
        message: "Image Uploaded Successfully",
        result,
      });
    } catch (error) {
      console.error("Error in Image Upload:", error);
      return res.status(500).json({
        success: false,
        message: "Internal Server Error",
        error: error.message,
      });
    }
  },
];
