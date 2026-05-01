import { Router } from "express";
import express from "express";
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

// Middleware to parse text/plain payloads sent by navigator.sendBeacon
router.use(express.text({ type: "text/plain" }));

router.post("/", async (req, res) => {
  let bodyData = req.body;
  if (typeof bodyData === "string") {
    try {
      bodyData = JSON.parse(bodyData);
    } catch (e) {
      console.error("[DELETE_BATCH] Failed to parse text/plain body:", e);
    }
  }

  const { submissionId } = bodyData || {};

  if (!submissionId) {
    return res.status(400).json({ message: "Missing submissionId" });
  }

  try {
    // 1. Find ALL files in MongoDB matching this submissionId
    const files = await Upload.find({ submissionId });

    if (!files || files.length === 0) {
      return res.status(200).json({ message: "No files found to delete in DB" });
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
          console.log(`[DELETE_BATCH] Successfully deleted physical file ${key} from S3`);
        } catch (s3Error) {
          console.error(`[DELETE_BATCH] Error deleting file ${file.s3Url} from S3:`, s3Error);
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
            console.log(`[DELETE_BATCH] Temp TUS S3 objects deleted`);
        } catch(e) {}
      }

      // 3. Delete from MongoDB
      await Upload.findByIdAndDelete(file._id);
      console.log(`[DELETE_BATCH] Successfully deleted DB record ${file._id}`);
    }

    return res.status(200).json({ message: "File(s) batch deleted successfully" });
  } catch (error) {
    console.error(`[DELETE_BATCH] Error processing delete batch:`, error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

export default router;
