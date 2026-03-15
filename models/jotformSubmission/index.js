import mongoose from "mongoose";
import jotformSubmissionSchema from "./schema";

//User model created
const JotformSubmission = mongoose.model(
  "JotformSubmission",
  jotformSubmissionSchema
);

export default JotformSubmission;
