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
    lastManualReminderSentAt: {
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
      index: true,
    },
    propertyName: {
      type: String,
      default: "",
      index: true,
    },
    unitName: {
      type: String,
      default: "",
      index: true,
    },
    successFullUploadReminder: {
      type: Boolean,
      default: false,
    },
    isSubmited: {
      type: Boolean,
      default: false,
    },
    requiresUpload: {
      type: Boolean,
      default: true,
      index: true,
    },
    isDuplicateResolved: {
      type: Boolean,
      default: false,
      index: true,
    },
  },
  { timestamps: true }
);

// Compound index for instant duplicate grouping
jotformSubmissionSchema.index({
  isDuplicateResolved: 1,
  formName: 1,
  propertyName: 1,
  unitName: 1,
});

export default jotformSubmissionSchema;
// export default mongoose.model(
//   "JotformSubmission",
//   jotformSubmissionSchema
// );
