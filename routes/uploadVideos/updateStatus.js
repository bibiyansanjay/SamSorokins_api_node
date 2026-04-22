import { Router } from "express";
import { Upload } from "../../models/index.js";

const router = Router();

router.put("/", async (req, res) => {
  try {
    const { submissionId, filename, size, uploadId, status, failureReason } = req.body;

    if (!status) {
      return res.status(400).json({ success: false, message: "Status is required" });
    }

    let filter = {};
    if (uploadId) {
      filter = { uploadId };
    } else if (submissionId && filename && size) {
      filter = { submissionId, filename, size };
    } else {
      return res.status(400).json({ success: false, message: "Must provide either uploadId or (submissionId, filename, size)" });
    }

    const updateData = { status };
    if (failureReason !== undefined) {
      updateData.failureReason = failureReason;
    }

    // Use updateMany in case there are edge-case duplicates, though findOneAndUpdate is also fine.
    // Let's use findOneAndUpdate to just hit the latest one (sorting by createdAt desc).
    const updatedUpload = await Upload.findOneAndUpdate(
      filter,
      { $set: updateData },
      { new: true, sort: { createdAt: -1 } }
    );

    if (!updatedUpload) {
      return res.status(404).json({ success: false, message: "Upload record not found" });
    }

    console.log(`[StatusUpdate] Updated record ${updatedUpload._id} to ${status}. Reason: ${failureReason || 'N/A'}`);

    return res.status(200).json({ success: true, upload: updatedUpload });
  } catch (error) {
    console.error("[StatusUpdate] Error:", error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

export default router;
