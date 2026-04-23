import { Router } from "express";
import { S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "../../models/index.js";

const router = Router();

const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

router.delete("/", async (req, res) => {
  const { filename, submissionId } = req.query;

  if (!filename || !submissionId) {
    return res.status(400).json({ message: "Missing filename or submissionId" });
  }

  try {
    // 1. Find all files in MongoDB matching these criteria
    // (There should only be one, but just in case we handle it)
    const files = await Upload.find({ filename, submissionId });

    if (!files || files.length === 0) {
      return res.status(404).json({ message: "File not found in DB" });
    }

    for (const file of files) {
      // 2. Extract S3 key and delete from S3
      if (file.s3Url) {
        try {
          const urlObj = new URL(file.s3Url);
          const bucket = urlObj.hostname.split(".")[0];
          const key = decodeURIComponent(urlObj.pathname.substring(1));

          const deleteCommand = new DeleteObjectCommand({
            Bucket: process.env.AWS_BUCKET || bucket,
            Key: key,
          });

          await s3Client.send(deleteCommand);
          console.log(`[DELETE_BY_NAME] Successfully deleted physical file ${key} from S3`);
        } catch (s3Error) {
          console.error(`[DELETE_BY_NAME] Error deleting file ${file.s3Url} from S3:`, s3Error);
        }
      }

      // Also clean up partial TUS files if upload was incomplete
      if (file.uploadId && file.status !== "Uploaded") {
        try {
            await s3Client.send(
                new DeleteObjectCommand({
                Bucket: process.env.AWS_BUCKET,
                Key: file.uploadId,
                })
            );
            await s3Client.send(
                new DeleteObjectCommand({
                Bucket: process.env.AWS_BUCKET,
                Key: `${file.uploadId}.info`,
                })
            );
            console.log(`[DELETE_BY_NAME] Temp TUS S3 objects deleted`);
        } catch(e) {}
      }

      // 3. Delete from MongoDB
      await Upload.findByIdAndDelete(file._id);
      console.log(`[DELETE_BY_NAME] Successfully deleted DB record ${file._id}`);
    }

    return res.status(200).json({ message: "File(s) deleted successfully" });
  } catch (error) {
    console.error(`[DELETE_BY_NAME] Error processing delete for ${filename}:`, error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

export default router;
