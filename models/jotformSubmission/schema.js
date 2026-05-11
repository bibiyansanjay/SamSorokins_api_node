import mongoose from "mongoose";

const jotformSubmissionSchema = new mongoose.Schema(
  {
    submissionId: {
      type: String,
      required: true,
      unique: true,
    },
    formId: String,
    ip: String,
    status: String,
    // createdAt: String,
    // updatedAt: String,

    // store all answers
    answers: {
      type: mongoose.Schema.Types.Mixed,
    },

    // store full raw payload (optional but useful)
    raw: {
      type: mongoose.Schema.Types.Mixed,
    },

    // Reminder tracking
    reminder1SentAt: {
      type: Date,
      default: null,
    },
    reminder2SentAt: {
      type: Date,
      default: null,
    },
    uploadRemindersStage: {
      type: Number,
      default: 0,
    },
    markedFailedAt: {
      type: Date,
      default: null,
    },
    uniqueId: {
      type: String,
      default: "",
    },
    replyEmail: {
      type: String,
      default: "",
    },
    formName: {
      type: String,
      default: "",
    },
    successFullUploadReminder: {
      type: Boolean,
      default: false,
    },
    isSubmited: {
      type: Boolean,
      default: false,
    },
  },
  { timestamps: true }
);

export default jotformSubmissionSchema;
// export default mongoose.model(
//   "JotformSubmission",
//   jotformSubmissionSchema
// );
