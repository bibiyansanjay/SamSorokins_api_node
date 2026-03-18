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

        // @tus/server already decodes base64 metadata values before passing to hooks.
        // Do NOT re-decode - it corrupts normal strings like "Sanjay" or numeric IDs.
        const metadata = upload.metadata || {};
        const { submissionId, userId, filename, filetype, residentName, residentEmail } = metadata;

        console.log(`[TUS] [${upload.id}] Metadata:`, {
          submissionId,
          userId,
          residentName,
          residentEmail,
          filename,
          filetype,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // Link to existing "Pending" record or create a new one
        try {
          console.log(`[TUS] [${upload.id}] 🔍 Attempting atomic link: submissionId=${submissionId}, filename=${filename}, size=${upload.size}`);
          
          if (submissionId && filename) {
            // Use findOneAndUpdate to atomically claim a Pending record
            const updatedRecord = await Upload.findOneAndUpdate(
              {
                submissionId,
                filename,
                size: upload.size,
                status: "Pending" // Only link to truly Pending records
              },
              {
                $set: {
                  uploadId: upload.id,
                  status: "Uploading"
                }
              },
              { new: true }
            );

            if (updatedRecord) {
              console.log(`[TUS] [${upload.id}] 🔗 Atomically linked to record (${updatedRecord._id})`);
            } else {
              console.log(`[TUS] [${upload.id}] ℹ️ No Pending record found. Checking for existing Uploading/Failed record to re-link...`);
              
              // On page refresh, the TUS ID may change but the file is the same.
              // Try to re-link an existing in-progress record (e.g., from a previous session).
              const existingRecord = await Upload.findOneAndUpdate(
                {
                  submissionId,
                  filename,
                  size: upload.size,
                  // NO STATUS FILTER: Re-link to ANY existing record (even "Uploaded").
                  // If the user spams uploads or explicitly re-uploads, we recycle the existing 
                  // record to guarantee exactly ONE DB row per (submissionId, filename).
                },
                {
                  $set: {
                    uploadId: upload.id, // Update to new TUS ID
                    status: "Uploading"
                  }
                },
                { new: true }
              );

              if (existingRecord) {
                console.log(`[TUS] [${upload.id}] 🔗 Re-linked to existing record (${existingRecord._id}) — was ${existingRecord.status}`);
              } else {
                // Truly new upload with no DB record at all — create one
                const newUpload = new Upload({
                  uploadId: upload.id,
                  submissionId,
                  userId: userId || null,
                  residentName: residentName || null,
                  residentEmail: residentEmail || null,
                  filename,
                  size: upload.size,
                  status: "Uploading",
                });
                await newUpload.save();
                console.log(`[TUS] [${upload.id}] ✅ New DB record created (no existing record found)`);
              }
            }
          } else {
            console.warn(`[TUS] [${upload.id}] ⚠️ Metadata missing for record linking: submissionId=${submissionId}, filename=${filename}`);
          }
        } catch (dbError) {
          console.error(`[TUS] [${upload.id}] ❌ DB Error on create:`, dbError.message);
        }

        return {}; // @tus/server v2 requires an object return
      },
      onUploadFinish: async (req, upload) => {
        console.log(`[TUS] [${upload.id}] ✅ Upload FINISH triggered`);

        // @tus/server already decodes base64 metadata values before passing to hooks.
        const metadata = upload.metadata || {};
        const { submissionId, userId, filename, filetype, residentName, residentEmail } = metadata;
        console.log(`[TUS] [${upload.id}] Finish Metadata:`, { submissionId, userId, filename, residentName, residentEmail });

        // Get S3 details
        // NOTE: @tus/s3-store stores the upload data at the upload.id as the S3 key.
        const s3Key = upload.id;
        console.log(`[TUS] [${upload.id}] 🔑 Raw upload.id (S3 source key): "${s3Key}"`);

        // CRITICAL: Ensure unique final key using upload.id to avoid collisions
        const timestamp = Date.now();
        const safeFilename = filename ? filename.replace(/\s+/g, "_") : "unnamed_file";
        const finalKey = `files/${timestamp}_${upload.id}_${safeFilename}`;

        const region = process.env.AWS_REGION || "us-east-1";
        const s3Url = `https://${process.env.AWS_BUCKET}.s3.${region}.amazonaws.com/${finalKey}`;

        let s3MoveSuccess = false;

        try {
          if (upload.size > 5 * 1024 * 1024 * 1024) {
             console.error(`[TUS] [${upload.id}] ❌ File exceeds 5GB CopyObject limit.`);
             throw new Error("File size exceeds S3 CopyObject limit (5GB)");
          }

          console.log(`[TUS] [${upload.id}] 🔄 Moving S3 Object from "${s3Key}" to "${finalKey}" (${(upload.size / (1024*1024)).toFixed(2)} MB)`);
          
          // CopySource: "bucket/key" — NO encoding needed for standard UUID-based TUS upload IDs
          const copyResult = await s3Client.send(
            new CopyObjectCommand({
              Bucket: process.env.AWS_BUCKET,
              CopySource: `${process.env.AWS_BUCKET}/${s3Key}`,
              Key: finalKey,
              // Set the correct MIME type so browsers display files instead of downloading them.
              // MetadataDirective: "REPLACE" is required to actually change the ContentType on copy.
              ContentType: filetype || "application/octet-stream",
              MetadataDirective: "REPLACE",
            })
          );
          console.log(`[TUS] [${upload.id}] ✅ S3 Copy success (RequestId: ${copyResult.$metadata.requestId})`);

          // Only delete originals if copy succeeded
          console.log(`[TUS] [${upload.id}] 🗑️ Deleting temp S3 objects: ${s3Key}, ${s3Key}.info`);
          await s3Client.send(new DeleteObjectCommand({ Bucket: process.env.AWS_BUCKET, Key: s3Key }));
          await s3Client.send(new DeleteObjectCommand({ Bucket: process.env.AWS_BUCKET, Key: `${s3Key}.info` }));
          console.log(`[TUS] [${upload.id}] ✅ Temp S3 objects deleted`);

          s3MoveSuccess = true;
        } catch (s3Error) {
          // Log the full error for diagnosis, not just the message
          console.error(`[TUS] [${upload.id}] ❌ S3 Move/Delete Error:`, {
            message: s3Error.message,
            code: s3Error.Code || s3Error.code,
            statusCode: s3Error.$metadata?.httpStatusCode,
            requestId: s3Error.$metadata?.requestId,
            key: s3Key,
            bucket: process.env.AWS_BUCKET,
          });
        }

        console.log(`[TUS] [${upload.id}] Finalizing DB record... (s3MoveSuccess=${s3MoveSuccess})`);

        // Update database with completed upload
        try {
          if (upload.id) {
            const updateData = {
              status: s3MoveSuccess ? "Uploaded" : "Failed",
              // Only set s3Url if the file was actually moved successfully
              ...(s3MoveSuccess && { s3Url }),
            };
            
            if (submissionId) updateData.submissionId = submissionId;
            if (userId) updateData.userId = userId;
            if (residentName) updateData.residentName = residentName;
            if (residentEmail) updateData.residentEmail = residentEmail;

            const result = await Upload.findOneAndUpdate(
              { uploadId: upload.id },
              updateData,
              { new: true }
            );

            if (result) {
              console.log(`[TUS] [${upload.id}] ✅ DB updated: status=${result.status}, url=${result.s3Url || "none"}`);
            } else {
              console.warn(`[TUS] [${upload.id}] ⚠️ No record found by uploadId="${upload.id}" on finish. Creating fallback...`);
              const newUpload = new Upload({
                uploadId: upload.id,
                submissionId: submissionId || "unknown",
                userId: userId || null,
                residentName: residentName || null,
                residentEmail: residentEmail || null,
                filename: filename || "unnamed_file",
                size: upload.size,
                status: s3MoveSuccess ? "Uploaded" : "Failed",
                ...(s3MoveSuccess && { s3Url }),
              });
              await newUpload.save();
              console.log(`[TUS] [${upload.id}] ✅ DB fallback insert: ${newUpload._id}`);
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

        return {}; // @tus/server v2 requires an object return to prevent patch.status_code destructuring crash
      },
    });

    console.log("✅ TUS Server initialized successfully");
  }
  return tusServer;
};

export default getTusServer;
