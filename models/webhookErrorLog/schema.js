import mongoose from "mongoose";

const webhookErrorLogSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      default: "",
    },
    formId: {
      type: String,
      default: "",
    },
    submissionID: {
      type: String,
      default: "",
    },
    type: {
      type: String,
      default: "",
    },
    reason: {
      type: String,
      default: "",
    },
    details: {
      type: mongoose.Schema.Types.Mixed,
    },
  },
  { timestamps: true }
);

export default webhookErrorLogSchema;
