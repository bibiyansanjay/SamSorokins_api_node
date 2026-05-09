import mongoose from "mongoose";
import webhookErrorLogSchema from "./schema.js";

const WebhookErrorLog = mongoose.model(
  "WebhookErrorLog",
  webhookErrorLogSchema
);

export default WebhookErrorLog;
