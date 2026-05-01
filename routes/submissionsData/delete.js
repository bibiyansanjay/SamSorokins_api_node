import { S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { Upload, JotformSubmission } from "../../models/index.js";

export default async (req, res, next) => {
  const { submissionId } = req.params;

  if (!submissionId) {
    return res.status(400).json({ message: "Missing submissionId" });
  }

  const s3Client = new S3Client({
    region: process.env.AWS_REGION || "us-east-1",
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });

  try {
    // 1. Find ALL files in MongoDB matching this submissionId
    const files = await Upload.find({ submissionId });

    for (const file of files) {
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
          console.log(`[DELETE_SUBMISSION] Successfully deleted physical file ${key} from S3`);
        } catch (s3Error) {
          console.error(`[DELETE_SUBMISSION] Error deleting file ${file.s3Url} from S3:`, s3Error);
        }
      }

      // Also clean up partial TUS files if upload was incomplete
      if (file.uploadId && file.status !== "Uploaded") {
        try {
            await s3Client.send(
                new DeleteObjectCommand({
                Bucket: process.env.AWS_BUCKET,
                Key: file.uploadId,
                })
            );
            await s3Client.send(
                new DeleteObjectCommand({
                Bucket: process.env.AWS_BUCKET,
                Key: `${file.uploadId}.info`,
                })
            );
            console.log(`[DELETE_SUBMISSION] Temp TUS S3 objects deleted`);
        } catch(e) {}
      }

      // 3. Delete from MongoDB
      await Upload.findByIdAndDelete(file._id);
      console.log(`[DELETE_SUBMISSION] Successfully deleted Upload DB record ${file._id}`);
    }

    // 4. Delete the JotformSubmission
    const deletedSubmission = await JotformSubmission.findOneAndDelete({ submissionId });
    if (deletedSubmission) {
        console.log(`[DELETE_SUBMISSION] Successfully deleted JotformSubmission record ${submissionId}`);
    } else {
        console.log(`[DELETE_SUBMISSION] JotformSubmission record ${submissionId} not found`);
    }

    return res.status(200).json({ success: true, message: "Submission deleted successfully" });
  } catch (error) {
    console.error(`[DELETE_SUBMISSION] Error processing delete submission:`, error);
    next(error);
  }
};
