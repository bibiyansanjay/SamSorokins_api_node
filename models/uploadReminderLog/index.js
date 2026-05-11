import mongoose from "mongoose";
import uploadReminderLogSchema from "./schema.js";

const UploadReminderLog = mongoose.model(
  "UploadReminderLog",
  uploadReminderLogSchema
);

export default UploadReminderLog;
