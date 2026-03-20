import { Router } from "express";
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

router.delete("/:id", async (req, res) => {
  const { id } = req.params;

  try {
    // 1. Find the file in MongoDB
    const file = await Upload.findById(id);

    if (!file) {
      return res.status(404).json({ message: "File not found" });
    }

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
        console.log(`[DELETE] Successfully deleted physical file ${key} from S3`);
      } catch (s3Error) {
        // Log S3 error but continue to delete the DB record to avoid zombie records
        console.error(`[DELETE] Error deleting file ${file.s3Url} from S3:`, s3Error);
      }
    }

    // 3. Delete from MongoDB
    await Upload.findByIdAndDelete(id);
    console.log(`[DELETE] Successfully deleted DB record ${id}`);

    return res.status(200).json({ message: "File deleted successfully" });
  } catch (error) {
    console.error(`[DELETE] Error processing delete for ID ${id}:`, error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

export default router;
