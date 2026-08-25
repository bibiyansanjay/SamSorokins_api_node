import mongoose from "mongoose";

const requestLogSchema = new mongoose.Schema(
  {
    requestName: {
      type: String,
      default: "",
    },
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Users",
      default: null,
    },
    email: {
      type: String,
      default: "",
    },
    ipAddress: {
      type: String,
      default: "",
    },
    method: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    },
    statusCode: {
      type: Number,
      default: 200,
    },
    responseTime: {
      type: Number,
      default: 0,
    },
    userAgent: {
      type: String,
      default: "",
    },
  },
  { timestamps: true }
);

export default requestLogSchema;
