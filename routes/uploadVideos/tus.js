import { Server } from "@tus/server";
import { S3Store } from "@tus/s3-store";
import { Upload } from "../../models/index.js";
import { Worker } from "worker_threads";
import path from "path";
// import pool from "../../db";
import { S3Client, CopyObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
dotenv.config();

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

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
          client: s3Client,
          bucket: process.env.AWS_BUCKET,
        },
        partSize: 10 * 1024 * 1024,
        debug: true,
      }),

      onUploadCreate: async (req, upload) => {
        console.log("📤 Upload CREATE triggered");
        if (!upload) {
          console.error("❌ upload object missing");
          return;
        }
        console.log("upload.metadata", upload.metadata);
        const metadata = upload.metadata || {};
        const { submissionId, userId, filename, filetype } = metadata;

        console.log("Upload metadata:", {
          uploadId: upload.id,
          submissionId,
          userId: "69abe4b1284af79b9b23165d",
          filename,
          filetype,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // Optional: Save initial record to database
        try {
          if ((submissionId || userId) && filename) {
            const newUpload = new Upload({
              uploadId: upload.id,
              submissionId,
              userId: "69abe4b1284af79b9b23165d",
              filename,
              size: upload.size,
              status: "uploading",
            });
            await newUpload.save();
            console.log("✅ Initial DB record created");
          } else {
            console.log("⚠️ Missing required metadata for DB record", {
              submissionId,
              userId,
              filename,
            });
          }
        } catch (dbError) {
          console.error("❌ DB Error on create:", dbError);
        }

        return { res: { status: 200 } };
      },
      onUploadFinish: async (req, upload) => {
        console.log("✅ Upload FINISH triggered");

        const metadata = upload.metadata || {};
        const { submissionId, userId, filename, filetype } = metadata;

        // Get S3 details
        const s3Key = upload.id;
        const finalKey = filename ? `${Date.now()}_${filename.replace(/\s+/g, "_")}` : `${upload.id}.mp4`;
        const s3Url = `https://${process.env.AWS_BUCKET}.s3.${process.env.AWS_REGION || "us-east-1"
          }.amazonaws.com/${finalKey}`;

        try {
          console.log(`🔄 Copying AWS Object from ${s3Key} to ${finalKey} with Content-Type ${filetype || 'video/mp4'}`);
          await s3Client.send(new CopyObjectCommand({
            Bucket: process.env.AWS_BUCKET,
            CopySource: `${process.env.AWS_BUCKET}/${s3Key}`,
            Key: finalKey,
            ContentType: filetype || 'video/mp4',
            MetadataDirective: 'REPLACE'
          }));

          console.log(`🗑️ Deleting temporary TUS files: ${s3Key} and ${s3Key}.info`);
          await s3Client.send(new DeleteObjectCommand({
            Bucket: process.env.AWS_BUCKET,
            Key: s3Key
          }));
          await s3Client.send(new DeleteObjectCommand({
            Bucket: process.env.AWS_BUCKET,
            Key: `${s3Key}.info`
          }));
        } catch (s3Error) {
          console.error("❌ S3 Copy/Delete Error:", s3Error);
        }

        console.log("Upload completed successfully:", {
          uploadId: upload.id,
          submissionId,
          userId: "69abe4b1284af79b9b23165d",
          filename: finalKey,
          s3Url,
          s3Key: finalKey,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // Update database with completed upload
        try {
          console.log(s3Url, finalKey, "completed", submissionId, filename);

          if (upload.id) {
            const result = await Upload.findOneAndUpdate(
              { uploadId: upload.id },
              { status: "completed", s3Url, filename: finalKey },
              { new: true }
            );

            if (result) {
              console.log("✅ Database updated successfully", result._id);
            } else {
              // Fallback insert if record was missing
              const newUpload = new Upload({
                uploadId: upload.id,
                submissionId: submissionId || "unknown",
                userId: "69abe4b1284af79b9b23165d" || "unknown",
                filename: finalKey,
                size: upload.size,
                status: "completed",
                s3Url,
              });
              await newUpload.save();
              console.log("✅ Database insert successful (fallback)");
            }
          }
        } catch (dbError) {
          console.error("❌ DB Error on finish:", dbError);
        }

        // Spawn a Worker Thread for heavy post-processing tasks
        try {
          const workerPath = path.join(
            __dirname,
            "../../workers/videoProcessor.js"
          );
          const worker = new Worker(workerPath, {
            workerData: {
              s3Url,
              size: upload.size,
              uploadId: upload.id,
              filename: finalKey,
            },
          });

          worker.on("message", (msg) => {
            console.log(
              `[Worker Message] ${msg.uploadId}:`,
              msg.message || msg.error
            );
          });

          worker.on("error", (err) => {
            console.error(`[Worker Error] for upload ${upload.id}:`, err);
          });

          worker.on("exit", (code) => {
            console.log(
              `[Worker Exit] Worker for upload ${upload.id} exited with code ${code}`
            );
          });
        } catch (workerError) {
          console.error("❌ Failed to spawn background worker:", workerError);
        }

        return { res: { status: 200 } };
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
