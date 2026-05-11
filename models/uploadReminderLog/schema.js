import mongoose from "mongoose";

const uploadReminderLogSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      default: "",
    },
    formId: {
      type: String,
      default: "",
    },
    submissionId: {
      type: String,
      default: "",
    },
    reminderType: {
      type: String,
      default: "",
    },
  },
  {
    timestamps: true,
  }
);

export default uploadReminderLogSchema;
