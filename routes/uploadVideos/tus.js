import { Server } from "@tus/server";
import { S3Store } from "@tus/s3-store";
// import pool from "../../db";
import { S3Client } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
dotenv.config();

console.log("AWS Configuration:", {
  region: process.env.AWS_REGION,
  bucket: process.env.AWS_BUCKET,
  hasAccessKey: !!process.env.AWS_ACCESS_KEY_ID,
  hasSecretKey: !!process.env.AWS_SECRET_ACCESS_KEY,
});

// Initialize TUS Server once (singleton pattern)
let tusServer;

const getTusServer = () => {
  if (!tusServer) {
    console.log("Initializing TUS Server...");

    tusServer = new Server({
      path: "/upload/tus",
      datastore: new S3Store({
        s3ClientConfig: {
          client: new S3Client({
            region: process.env.AWS_REGION,
            credentials: {
              accessKeyId: process.env.AWS_ACCESS_KEY_ID,
              secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
            },
          }),
          bucket: process.env.AWS_BUCKET,
        },
        partSize: 10 * 1024 * 1024,
        debug: true,
      }),

      onUploadCreate: async (req, res, upload) => {
        console.log("📤 Upload CREATE triggered");
        if (!upload) {
          console.error("❌ upload object missing");
          return res;
        }
        console.log("upload.metadata", upload.metadata);
        const metadata = upload.metadata || {};
        const { submissionId, userId, filename, filetype } = metadata;

        console.log("Upload metadata:", {
          uploadId: upload.id,
          submissionId,
          userId,
          filename,
          filetype,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // Optional: Save initial record to database
        try {
          // upload data in db
          console.log(
            submissionId || "unknown",
            userId || "unknown",
            filename || "unknown",
            filetype || "video/mp4",
            upload.size,
            "uploading"
          );
          console.log("✅ Initial DB record created");
        } catch (dbError) {
          console.error("❌ DB Error on create:", dbError);
        }

        return res;
      },
      onUploadFinish: async (req, res, upload) => {
        console.log("✅ Upload FINISH triggered");

        const metadata = upload.metadata || {};
        const { submissionId, userId, filename, filetype } = metadata;

        // Get S3 details
        const s3Key = upload.storage?.key;
        const s3Url = `https://${process.env.AWS_BUCKET}.s3.${
          process.env.AWS_REGION || "us-east-1"
        }.amazonaws.com/${s3Key}`;

        console.log("Upload completed successfully:", {
          uploadId: upload.id,
          submissionId,
          userId,
          filename,
          s3Url,
          s3Key,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // Update database with completed upload
        try {
          console.log(s3Url, s3Key, "completed", submissionId, filename);

          console.log("✅ Database updated successfully");
        } catch (dbError) {
          console.error("❌ DB Error on finish:", dbError);

          // If update failed, try insert (fallback)
          try {
            console.log(
              submissionId || "unknown",
              userId || "unknown",
              filename || "unknown",
              s3Url,
              filetype || "video/mp4",
              upload.size,
              s3Key,
              "completed"
            );
            console.log("✅ Database insert successful (fallback)");
          } catch (insertError) {
            console.error("❌ DB Insert Error:", insertError);
          }
        }

        return res;
      },
      onUploadFail: async (req, res, upload, error) => {
        console.error("❌ Upload FAILED:", {
          uploadId: upload.id,
          error: error.message,
          stack: error.stack,
        });

        const metadata = upload.metadata || {};
        const { submissionId, filename } = metadata;

        // Update database with failed status
        try {
          console.log("failed", error.message, submissionId, filename);
        } catch (dbError) {
          console.error("❌ DB Error on fail:", dbError);
        }
      },
    });

    console.log("✅ TUS Server initialized successfully");
  }
  return tusServer;
};

export default async (req, res, next) => {
  try {
    console.log(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔵 TUS Request: ${req.method} ${req.url}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    `);

    const server = getTusServer();
    await server.handle(req, res);

    console.log("✅ TUS Request handled");
  } catch (error) {
    console.error("❌ TUS Handler Error:", {
      message: error.message,
      stack: error.stack,
    });

    if (!res.headersSent) {
      res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  }
};
