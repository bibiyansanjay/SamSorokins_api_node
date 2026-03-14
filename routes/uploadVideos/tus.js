import { Upload } from "../../models/index.js";
import { Worker } from "worker_threads";
import path from "path";
import {
  S3Client,
  CopyObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";


// import dotenv from "dotenv";
// dotenv.config();

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

const getTusServer = async () => {
  if (!tusServer) {
    console.log("Initializing TUS Server...");

    const { Server } = await import("@tus/server");
    const { S3Store } = await import("@tus/s3-store");

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
        console.log(`[TUS] [${upload.id}] 📤 Upload CREATE triggered`);
        if (!upload) {
          console.error(`[TUS] [${upload.id}] ❌ upload object missing`);
          return;
        }

        const metadata = upload.metadata || {};
        // Note: TUS metadata values are usually base64 encoded strings in raw TUS, 
        // but @tus/server often decodes them. We'll use them as is first.
        const { submissionId, userId, filename, filetype } = metadata;

        console.log(`[TUS] [${upload.id}] Metadata:`, {
          submissionId,
          userId,
          filename,
          filetype,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // Link to existing "Pending" record or create a new one
        try {
          console.log(`[TUS] [${upload.id}] 🔍 Searching for Pending record: submissionId=${submissionId}, filename=${filename}`);
          if (submissionId && filename) {
            const existingPending = await Upload.findOne({
              submissionId,
              filename,
              status: "Pending"
            });

            if (existingPending) {
              console.log(`[TUS] [${upload.id}] 🔗 Found existing Pending record (${existingPending._id}), linking...`);
              existingPending.uploadId = upload.id;
              existingPending.status = "Uploading";
              await existingPending.save();
              console.log(`[TUS] [${upload.id}] ✅ Pending record linked successfully`);
            } else {
              console.log(`[TUS] [${upload.id}] ℹ️ No Pending record found, creating new record...`);
              const newUpload = new Upload({
                uploadId: upload.id,
                submissionId,
                userId: userId || null,
                filename,
                size: upload.size,
                status: "Uploading",
              });
              await newUpload.save();
              console.log(`[TUS] [${upload.id}] ✅ New DB record created (fallback)`);
            }
          } else {
            console.warn(`[TUS] [${upload.id}] ⚠️ Metadata missing for record linking: submissionId=${submissionId}, filename=${filename}`);
          }
        } catch (dbError) {
          console.error(`[TUS] [${upload.id}] ❌ DB Error on create:`, dbError.message);
        }

        return { res: { status: 200 } };
      },
      onUploadFinish: async (req, upload) => {
        console.log(`[TUS] [${upload.id}] ✅ Upload FINISH triggered`);

        const metadata = upload.metadata || {};
        const { submissionId, userId, filename, filetype } = metadata;
        console.log(`[TUS] [${upload.id}] Finish Metadata:`, { submissionId, userId, filename });

        // Get S3 details
        const s3Key = upload.id;

        // CRITICAL: Ensure unique final key using upload.id to avoid collisions
        const timestamp = Date.now();
        const safeFilename = filename ? filename.replace(/\s+/g, "_") : "unnamed_file";
        const finalKey = `files/${timestamp}_${upload.id}_${safeFilename}`;

        const s3Url = `https://${process.env.AWS_BUCKET}.s3.${process.env.AWS_REGION || "us-east-1"
          }.amazonaws.com/${finalKey}`;

        try {
          console.log(`[TUS] [${upload.id}] 🔄 Moving S3 Object to ${finalKey}`);
          await s3Client.send(
            new CopyObjectCommand({
              Bucket: process.env.AWS_BUCKET,
              CopySource: `${process.env.AWS_BUCKET}/${s3Key}`,
              Key: finalKey,
              ContentType: filetype || "application/octet-stream",
              MetadataDirective: "REPLACE",
            })
          );

          console.log(`[TUS] [${upload.id}] 🗑️ Deleting temp S3 keys: ${s3Key}, ${s3Key}.info`);
          await s3Client.send(
            new DeleteObjectCommand({
              Bucket: process.env.AWS_BUCKET,
              Key: s3Key,
            })
          );
          await s3Client.send(
            new DeleteObjectCommand({
              Bucket: process.env.AWS_BUCKET,
              Key: `${s3Key}.info`,
            })
          );
        } catch (s3Error) {
          console.error(`[TUS] [${upload.id}] ❌ S3 Move/Delete Error:`, s3Error.message);
        }

        console.log(`[TUS] [${upload.id}] Finalizing DB record...`);

        // Update database with completed upload
        try {
          if (upload.id) {
            const updateData = {
              status: "Uploaded",
              s3Url,
            };
            
            if (submissionId) updateData.submissionId = submissionId;
            if (userId) updateData.userId = userId;
            // We do NOT overwrite updateData.filename with finalKey anymore
            // to keep the "exact file name" in the database as requested.

            const result = await Upload.findOneAndUpdate(
              { uploadId: upload.id },
              updateData,
              { new: true }
            );

            if (result) {
              console.log(`[TUS] [${upload.id}] ✅ Database updated: status=Uploaded, id=${result._id}`);
            } else {
              console.log(`[TUS] [${upload.id}] ⚠️ Record missing on finish, creating fallback...`);
              const newUpload = new Upload({
                uploadId: upload.id,
                submissionId: submissionId || "unknown",
                userId: userId || null,
                filename: filename || "unnamed_file", // Use original filename
                size: upload.size,
                status: "Uploaded",
                s3Url,
              });
              await newUpload.save();
              console.log(`[TUS] [${upload.id}] ✅ Database fallback insert successful: ${newUpload._id}`);
            }
          }
        } catch (dbError) {
          console.error(`[TUS] [${upload.id}] ❌ DB Error on finish:`, dbError.message);
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
            console.log(`[TUS] [${upload.id}] [Worker Message]:`, msg.message || msg.error);
          });

          worker.on("error", (err) => {
            console.error(`[TUS] [${upload.id}] [Worker Error]:`, err.message);
          });

          worker.on("exit", (code) => {
            console.log(`[TUS] [${upload.id}] [Worker Exit] Code ${code}`);
          });
        } catch (workerError) {
          console.error(`[TUS] [${upload.id}] ❌ Failed to spawn background worker:`, workerError.message);
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

    const server = await getTusServer();
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
