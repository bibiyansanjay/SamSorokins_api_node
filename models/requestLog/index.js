import mongoose from "mongoose";
import requestLogSchema from "./schema.js";

const RequestLog = mongoose.model("RequestLog", requestLogSchema);

export default RequestLog;
