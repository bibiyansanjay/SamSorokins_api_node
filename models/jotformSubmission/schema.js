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
    createdAt: String,
    updatedAt: String,

    // store all answers
    answers: {
      type: mongoose.Schema.Types.Mixed,
    },

    // store full raw payload (optional but useful)
    raw: {
      type: mongoose.Schema.Types.Mixed,
    },
  },
  { timestamps: true }
);

export default jotformSubmissionSchema;
// export default mongoose.model(
//   "JotformSubmission",
//   jotformSubmissionSchema
// );
