import cron from "node-cron";
import mongoose from "mongoose";
import { Upload, User } from "../models";
import sendMail from "../methods/sendMail.js";

// Run every hour
const sendReminder = async () => {
  try {
    console.log(
      "[UploadReminder] Running cron job to check for stale uploads..."
    );

    // Find uploads that have been "uploading" for more than 24 hours
    const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const staleUploads = await Upload.find({
      status: "uploading",
      reminderSent: false,
      // createdAt: { $lte: twentyFourHoursAgo }
    }).populate("userId"); // Populate user data to get email

    if (staleUploads.length === 0) {
      console.log("[UploadReminder] No stale uploads found.");
      return;
    }

    console.log(
      `[UploadReminder] Found ${staleUploads.length} stale uploads. Process sending emails...`
    );

    for (const upload of staleUploads) {
      if (upload.userId && upload.userId.email) {
        const payload = {
          fName: upload.userId.name || "User",
          filename: upload.filename,
        };

        // Send Reminder Email
        await sendMail(
          "Action Required: Complete Your Video Uploads",
          payload,
          upload.userId.email,
          "template-uploadReminder",
          "",
          []
        );

        console.log(
          `[UploadReminder] Sent email to ${upload.userId.email} for file ${upload.filename}`
        );

        // Mark as reminder sent
        upload.reminderSent = true;
        await upload.save();
      }
    }
  } catch (error) {
    console.error("[UploadReminder] Error running reminder cron job:", error);
  }
};

//Run every hour
// "0 * * * *"
cron.schedule(
  "0 5 * * *", // Run every day at 5 AM California time
  async () => {
    console.log("Task executed:", new Date());
    await sendReminder();
  },
  {
    timezone: "America/Los_Angeles",
  }
);
