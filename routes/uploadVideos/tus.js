import { Server } from "@tus/server";
import { S3Store } from "@tus/s3-store";
import { S3Client } from "@aws-sdk/client-s3";

// Initialize S3 Client
const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Initialize TUS Server once (singleton pattern)
let tusServer;

const getTusServer = () => {
  if (!tusServer) {
    tusServer = new Server({
      path: "/upload/tus",
      datastore: new S3Store({
        s3Client: s3Client,
        bucket: process.env.AWS_S3_BUCKET_NAME || "your-bucket-name",
        partSize: 10 * 1024 * 1024, // 10MB chunks
      }),
      onUploadCreate: async (req, res, upload) => {
        const { submissionId, userId, filename } = upload.metadata;

        console.log("📤 Upload started:", {
          uploadId: upload.id,
          submissionId,
          userId,
          filename,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // TODO: Save to database
        // Example:
        // await Video.create({
        //   uploadId: upload.id,
        //   submissionId,
        //   userId,
        //   filename,
        //   fileSize: upload.size,
        //   status: 'uploading',
        //   createdAt: new Date()
        // });

        return res;
      },
      onUploadFinish: async (req, res, upload) => {
        const { submissionId, userId, filename } = upload.metadata;

        const s3Key = upload.storage?.key;
        const s3Url = `https://${process.env.AWS_S3_BUCKET_NAME}.s3.${
          process.env.AWS_REGION || "us-east-1"
        }.amazonaws.com/${s3Key}`;

        console.log("✅ Upload completed:", {
          uploadId: upload.id,
          submissionId,
          userId,
          filename,
          s3Url,
          s3Key,
          size: `${(upload.size / (1024 * 1024)).toFixed(2)} MB`,
        });

        // TODO: Update database with completed upload
        // Example:
        // await Video.update({
        //   uploadId: upload.id
        // }, {
        //   status: 'completed',
        //   s3Url: s3Url,
        //   s3Key: s3Key,
        //   completedAt: new Date()
        // });

        return res;
      },
      onUploadFail: async (req, res, upload, error) => {
        console.error("❌ Upload failed:", {
          uploadId: upload.id,
          error: error.message,
        });

        // TODO: Update database with failed status
        // Example:
        // await Video.update({
        //   uploadId: upload.id
        // }, {
        //   status: 'failed',
        //   error: error.message,
        //   failedAt: new Date()
        // });
      },
    });
  }
  return tusServer;
};

export default async (req, res, next) => {
  try {
    console.log("TUS Request:", req.method, req.url);

    const server = getTusServer();
    await server.handle(req, res);
  } catch (error) {
    console.error("TUS Error:", error);

    if (!res.headersSent) {
      res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  }
};
