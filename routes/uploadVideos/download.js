import { Router } from "express";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";

const router = Router();

// Create an S3 client instance
const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

router.get("/", async (req, res) => {
  const { url, filename } = req.query;

  if (!url) {
    return res.status(400).send("File URL is required");
  }

  try {
    // Extract bucket and key from the standard S3 URL
    // Format: https://bucket-name.s3.region.amazonaws.com/files/...
    const urlObj = new URL(url);
    const bucket = urlObj.hostname.split(".")[0];
    
    // The key is the path without the leading slash
    const key = decodeURIComponent(urlObj.pathname.substring(1));

    const command = new GetObjectCommand({
      Bucket: process.env.AWS_BUCKET || bucket,
      Key: key,
    });

    const s3Response = await s3Client.send(command);

    const safeFilename = filename || key.split('/').pop() || 'download';

    // Set headers to force download
    res.setHeader("Content-Disposition", `attachment; filename="${safeFilename}"`);
    res.setHeader("Content-Type", s3Response.ContentType || "application/octet-stream");
    if (s3Response.ContentLength) {
      res.setHeader("Content-Length", s3Response.ContentLength);
    }

    // Stream the file directly to the client
    s3Response.Body.pipe(res);
  } catch (error) {
    console.error("Error downloading file from S3:", error);
    res.status(500).send("Error downloading file");
  }
});

export default router;
