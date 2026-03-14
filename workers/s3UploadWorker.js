// workers/s3UploadWorker.js
// This runs in a separate thread — completely independent of the main server.
// Even if the browser closes, this worker keeps running until all files are uploaded.

import { workerData, parentPort } from "worker_threads";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import mongoose from "mongoose";
import { Upload } from "../models/index.js";

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const BUCKET = process.env.AWS_BUCKET;
const REGION = process.env.AWS_REGION || "us-east-1";

// ── Connect to MongoDB (worker has its own process context) ─────────────────
async function connectDB() {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect(process.env.MONGODB_URI);
    console.log("[Worker] MongoDB connected");
  }
}

// ── Upload a single file buffer to S3 ───────────────────────────────────────
async function uploadToS3(file) {
  const timestamp = Date.now();
  const safeFilename = file.originalname.replace(/\s+/g, "_");
  const s3Key = `uploads/${timestamp}_${safeFilename}`;

  await s3Client.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: s3Key,
      Body: file.buffer, // file bytes from memory
      ContentType: file.mimetype,
      ContentLength: file.size,
    })
  );

  // Standard S3 URL format
  const s3Url = `https://${BUCKET}.s3.${REGION}.amazonaws.com/${s3Key}`;
  return { s3Key, s3Url };
}

// ── Main worker logic ────────────────────────────────────────────────────────
async function run() {
  const { files, submissionId, userId } = workerData;

  await connectDB();

  let successCount = 0;
  let failedCount = 0;

  for (const file of files) {
    try {
      parentPort?.postMessage({
        type: "progress",
        filename: file.originalname,
        status: "uploading",
      });

      const { s3Key, s3Url } = await uploadToS3(file);

      // Save S3 URL to MongoDB
      await Upload.findByIdAndUpdate(file.uploadId, {
        status: "completed",
        s3Url,
        s3Key,
        completedAt: new Date(),
      });

      successCount++;

      parentPort?.postMessage({
        type: "progress",
        filename: file.originalname,
        status: "completed",
        s3Url,
      });

      console.log(`[Worker] ✅ Uploaded: ${file.originalname} → ${s3Url}`);
    } catch (err) {
      failedCount++;
      console.error(`[Worker] ❌ Failed: ${file.originalname}`, err.message);

      // Mark as failed in DB so frontend polling can detect it
      await Upload.findByIdAndUpdate(file.uploadId, {
        status: "failed",
        error: err.message,
      }).catch(() => {});

      parentPort?.postMessage({
        type: "progress",
        filename: file.originalname,
        status: "failed",
        error: err.message,
      });
    }
  }

  parentPort?.postMessage({
    type: "complete",
    successCount,
    failedCount,
  });

  // Clean up DB connection and exit
  await mongoose.disconnect();
  process.exit(0);
}

run().catch((err) => {
  console.error("[Worker] Fatal error:", err);
  process.exit(1);
});
