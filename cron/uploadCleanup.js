import cron from "node-cron";
import { Upload } from "../models/index.js";
import { S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";

dotenv.config();

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const cleanupOldUploads = async () => {
  console.log(
    "[UploadCleanup] Running 72-hour cleanup job at",
    new Date().toISOString()
  );

  const now = Date.now();
  // 72 hours ago
  const threshold = new Date(now - 72 * 60 * 60 * 1000);

  try {
    const expiredUploads = await Upload.find({
      status: { $in: ["Pending", "Uploading"] },
      createdAt: { $lte: threshold },
    });

    if (expiredUploads.length === 0) {
      console.log("[UploadCleanup] No expired uploads found.");
      return;
    }

    console.log(
      `[UploadCleanup] Found ${expiredUploads.length} expired uploads to clean up.`
    );

    for (const upload of expiredUploads) {
      // 1. Delete S3 raw chunks if uploadId is a standard TUS id
      // upload.uploadId is used by TUS as the S3 object key.
      if (upload.uploadId && !upload.uploadId.startsWith("pending_")) {
        try {
          console.log(
            `[UploadCleanup] Deleting orphaned S3 objects for ${upload.uploadId}`
          );

          await s3Client.send(
            new DeleteObjectCommand({
              Bucket: process.env.AWS_BUCKET,
              Key: upload.uploadId,
            })
          );

          await s3Client.send(
            new DeleteObjectCommand({
              Bucket: process.env.AWS_BUCKET,
              Key: `${upload.uploadId}.info`,
            })
          );
        } catch (s3Error) {
          console.error(
            `[UploadCleanup] S3 Delete Error for ${upload.uploadId}:`,
            s3Error.message
          );
          // We continue even if S3 delete fails (e.g., object already gone or permission error)
        }
      }

      // 2. Mark DB record as failed
      upload.status = "Failed";
      upload.failureReason = "Timeout after 72 hours";
      await upload.save();

      console.log(
        `[UploadCleanup] Marked upload ${upload._id} (${upload.filename}) as Failed.`
      );
    }

    console.log("[UploadCleanup] Cleanup job complete.");
  } catch (error) {
    console.error("[UploadCleanup] Fatal error:", error);
  }
};

// Run every hour
cron.schedule(
  "0 * * * *",
  async () => {
    await cleanupOldUploads();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

export default cleanupOldUploads;
