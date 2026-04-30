import { Router } from "express";
import multer from "multer";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "../../models/index.js";

const router = Router();

// Configure Multer for in-memory storage (thumbnails are very small)
const storage = multer.memoryStorage();
const upload = multer({ storage });

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

router.post("/", upload.single("thumbnail"), async (req, res) => {
  try {
    const { submissionId, filename } = req.body;
    const file = req.file;

    if (!file || !submissionId || !filename) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields: thumbnail file, submissionId, or filename",
      });
    }

    // Generate safe key for S3
    const safeFilename = filename.replace(/\s+/g, "_");
    // Thumbnails get stored in the /thumbnails folder
    const thumbnailKey = `thumbnails/${submissionId}_${safeFilename}.jpg`;

    // Upload directly to S3
    await s3Client.send(
      new PutObjectCommand({
        Bucket: process.env.AWS_BUCKET,
        Key: thumbnailKey,
        Body: file.buffer,
        ContentType: "image/jpeg",
        // Make sure it can be cached nicely
        CacheControl: "max-age=31536000", 
      })
    );

    const region = process.env.AWS_REGION || "us-east-1";
    const thumbnailUrl = `https://${process.env.AWS_BUCKET}.s3.${region}.amazonaws.com/${thumbnailKey}`;

    // Update the database record
    const updatedUpload = await Upload.findOneAndUpdate(
      { submissionId, filename },
      { $set: { thumbnailUrl } },
      { new: true, sort: { createdAt: -1 } }
    );

    console.log(`[Thumbnail] Successfully uploaded to ${thumbnailUrl}`);

    return res.status(200).json({
      success: true,
      thumbnailUrl,
      upload: updatedUpload,
    });
  } catch (error) {
    console.error("[Thumbnail] Upload error:", error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

export default router;
