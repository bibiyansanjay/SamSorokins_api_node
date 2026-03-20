import { Router } from "express";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import archiver from "archiver";
import { Upload } from "../../models/index.js";

const router = Router();

const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

router.get("/", async (req, res) => {
  const { submissionId } = req.query;

  if (!submissionId) {
    return res.status(400).send("submissionId is required");
  }

  try {
    // 1. Fetch all completed uploads for this submission
    const files = await Upload.find({
      submissionId,
      status: "Uploaded",
      s3Url: { $exists: true, $ne: "" },
    });

    if (!files || files.length === 0) {
      return res.status(404).send("No uploaded files found for this submission");
    }

    // 2. Set up headers for streaming ZIP response
    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="Submission_${submissionId}.zip"`
    );

    // 3. Initialize Archiver
    const archive = archiver("zip", {
      zlib: { level: 0 }, // Level 0 means basically STORE (no compression) — much faster for already compressed media (JPEGs, MP4s)
    });

    // Listen for all archive warnings/errors
    archive.on("warning", function (err) {
      if (err.code === "ENOENT") {
        console.warn("Archiver warning:", err);
      } else {
        throw err;
      }
    });

    archive.on("error", function (err) {
      console.error("Archiver error:", err);
      if (!res.headersSent) {
        res.status(500).send("Error generating zip");
      }
    });

    // Pipe the archiver directly to the HTTP response stream
    archive.pipe(res);

    // Track used filenames to prevent archive collisions
    const usedNames = new Set();
    let fileCount = 0;

    // 4. Loop through files, stream from S3, append to ZIP
    for (const fileDoc of files) {
      try {
        const urlObj = new URL(fileDoc.s3Url);
        const bucket = urlObj.hostname.split(".")[0];
        const key = decodeURIComponent(urlObj.pathname.substring(1));

        const command = new GetObjectCommand({
          Bucket: process.env.AWS_BUCKET || bucket,
          Key: key,
        });

        // We specifically DO NOT await the stream finishing, archiver handles backpressure.
        // We only await the init of the S3 GetObject stream.
        const s3Response = await s3Client.send(command);

        // Deduplicate filenames inside the ZIP
        let archiveFilename = fileDoc.filename;
        let counter = 1;
        while (usedNames.has(archiveFilename)) {
          const extensionMatch = fileDoc.filename.match(/\.[0-9a-z]+$/i);
          const extension = extensionMatch ? extensionMatch[0] : "";
          const baseName = fileDoc.filename.replace(extension, "");
          archiveFilename = `${baseName}_(${counter})${extension}`;
          counter++;
        }
        usedNames.add(archiveFilename);

        // Append the stream to the zip
        archive.append(s3Response.Body, { name: archiveFilename });
        fileCount++;
      } catch (s3Error) {
        console.error(`Error fetching file ${fileDoc.filename} from S3:`, s3Error.message);
        // Continue zipping other files even if one fails
      }
    }

    if (fileCount === 0) {
      // If we failed to get ANY streams from S3 (e.g., all 404 deleted)
      // and we haven't sent headers yet, return 404
      if (!res.headersSent) {
        return res.status(404).send("Could not retrieve file streams from AWS S3");
      }
      // If we already piped and started zipping, we must finish the empty zip
      // or append a dummy error text file
      archive.append("All requested files were deleted or unavailable on S3.", {
        name: "ERROR_NO_FILES.txt",
      });
    }

    // 5. Finalize the archive (this will finish the stream and close the response)
    archive.finalize();
  } catch (error) {
    console.error("Error generating zip:", error);
    if (!res.headersSent) {
      res.status(500).send("Internal Server Error generating zip");
    }
  }
});

export default router;
