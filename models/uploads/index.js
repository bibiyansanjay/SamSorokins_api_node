import mongoose from "mongoose";

const uploadSchema = new mongoose.Schema(
  {
    uploadId: {
      type: String,
      required: true,
      unique: true,
    },
    submissionId: {
      type: String,
      required: true,
    },
    userId: {
      type: String,
      // ref: "User",
      // required: true,
    },
    filename: {
      type: String,
      required: true,
    },
    size: {
      type: Number,
      required: true,
    },
    s3Url: {
      type: String,
    },
    status: {
      type: String,
      enum: ["Pending", "Uploading", "Uploaded", "Failed"],
      default: "Pending",
    },
    reminderSent: {
      type: Boolean,
      default: false,
    },
    reminder1SentAt: {
      type: Date,
      default: null,
    },
    reminder2SentAt: {
      type: Date,
      default: null,
    },
    residentName: {
      type: String,
    },
    residentEmail: {
      type: String,
    },
  },
  { timestamps: true }
);

export default mongoose.model("Upload", uploadSchema);
