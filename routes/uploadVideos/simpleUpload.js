// routes/upload.js
import express from "express";
import multer from "multer";
import { Worker } from "worker_threads";
import path from "path";
import { Upload } from "../../models/index.js";

const router = express.Router();
const workerPath = path.join(__dirname, "../../workers/s3UploadWorker.js");

// Store files in memory buffer (sent directly to S3, never written to disk)
const storage = multer.memoryStorage();
const upload = multer({
  storage,
  limits: {
    fileSize: 5 * 1024 * 1024 * 1024, // 5GB per file
    files: 10,
  },
  fileFilter: (req, file, cb) => {
    const allowed = ["video/", "image/"];
    const isAllowed = allowed.some((type) => file.mimetype.startsWith(type));
    if (isAllowed) {
      cb(null, true);
    } else {
      cb(new Error(`File type not allowed: ${file.mimetype}`));
    }
  },
});

/**
 * POST /upload/files
 *
 * Frontend sends all files + metadata in a single multipart/form-data request.
 * Server immediately responds with 200 + pending upload IDs.
 * A Worker thread handles the actual S3 upload + MongoDB save in the background.
 */
router.post("/files", upload.array("files", 10), async (req, res) => {
  try {
    const { submissionId, userId } = req.body;
    const files = req.files;

    if (!files || files.length === 0) {
      return res
        .status(400)
        .json({ success: false, error: "No files provided" });
    }

    console.log(
      `📦 Received ${files.length} file(s) for submission ${submissionId}`
    );

    // Create a pending DB record for each file immediately
    const pendingUploads = await Promise.all(
      files.map(async (file) => {
        const doc = new Upload({
          submissionId,
          userId,
          filename: file.originalname,
          size: file.size,
          mimetype: file.mimetype,
          status: "pending",
          s3Url: null,
        });
        await doc.save();
        return { _id: doc._id.toString(), filename: file.originalname };
      })
    );

    // ── Respond immediately to the browser ──────────────────────────────────
    // The browser gets success right away; S3 upload happens in background.
    res.status(200).json({
      success: true,
      message: "Files received. Uploading to storage in background.",
      uploads: pendingUploads,
    });

    // ── Spawn Worker to handle S3 upload + MongoDB update ───────────────────
    // Pass file buffers via workerData (shared memory, not copied for Buffers)
    // const workerPath = path.join(__dirname, "../../workers/s3UploadWorker.js");

    const worker = new Worker(workerPath, {
      workerData: {
        files: files.map((file, i) => ({
          uploadId: pendingUploads[i]._id,
          buffer: file.buffer, // raw file bytes
          originalname: file.originalname,
          mimetype: file.mimetype,
          size: file.size,
        })),
        submissionId,
        userId,
      },
    });

    worker.on("message", (msg) => {
      if (msg.type === "progress") {
        console.log(`[Worker] ${msg.filename}: ${msg.status}`);
      }
      if (msg.type === "complete") {
        console.log(
          `[Worker] All uploads done. Success: ${msg.successCount}, Failed: ${msg.failedCount}`
        );
      }
    });

    worker.on("error", (err) => {
      console.error("[Worker Error]", err);
    });

    worker.on("exit", (code) => {
      console.log(`[Worker] Exited with code ${code}`);
    });
  } catch (err) {
    console.error("Upload route error:", err);
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: err.message });
    }
  }
});

/**
 * GET /upload/status/:submissionId
 *
 * Frontend can poll this to check if background uploads are done.
 */
router.get("/status/:submissionId", async (req, res) => {
  try {
    const uploads = await Upload.find(
      { submissionId: req.params.submissionId },
      { filename: 1, status: 1, s3Url: 1, _id: 1 }
    );

    const total = uploads.length;
    const completed = uploads.filter((u) => u.status === "completed").length;
    const failed = uploads.filter((u) => u.status === "failed").length;
    const pending = uploads.filter((u) => u.status === "pending").length;

    res.json({
      success: true,
      total,
      completed,
      failed,
      pending,
      allDone: completed + failed === total,
      uploads,
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

export default router;
