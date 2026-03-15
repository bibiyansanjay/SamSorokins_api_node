import { Router } from "express";
import { Upload } from "../../models/index.js";

const router = Router();

router.post("/", async (req, res) => {
  try {
    console.log("[Batch] Received registration request:", req.body);
    const { submissionId, userId, files, residentName, residentEmail } = req.body;

    if (!submissionId || !files || !Array.isArray(files)) {
      return res.status(400).json({ success: false, message: "Invalid batch data" });
    }

    console.log(`[Batch] Registering ${files.length} files for submission ${submissionId}`);

    const ops = files.map((file) => ({
      updateOne: {
        filter: { submissionId, filename: file.name, size: file.size, status: "Pending" },
        update: {
          $setOnInsert: {
            uploadId: `pending_${Math.random().toString(36).substring(7)}`,
            submissionId,
            filename: file.name,
            size: file.size,
            status: "Pending",
            userId: userId || null,
            residentName: residentName || null,
            residentEmail: residentEmail || null,
          }
        },
        upsert: true
      }
    }));

    // Bulk write for idempotency
    await Upload.bulkWrite(ops);

    res.status(200).json({ success: true, message: "Batch registered successfully" });
  } catch (error) {
    console.error("[Batch Error]:", error);
    res.status(500).json({ success: false, error: error.message });
  }
});

export default router;
