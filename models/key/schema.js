import mongoose from "mongoose";

const keySchema = new mongoose.Schema(
  {
    rfid: {
      type: String,
      required: true,
      trim: true,
      unique: true,
    },
    communityCode: {
      type: String,
      required: true,
      trim: true,
    },
    unit: {
      type: String,
      trim: true,
      default: "",
    },
    types: {
      type: [String],
      required: true,
      default: [],
    },
    whoHasIt: {
      type: String,
      required: true,
      trim: true,
    },
    serviceRequest: {
      type: String,
      trim: true,
      default: "",
    },
    status: {
      type: String,
      enum: ["Key Requested", "Key Checked Out", "Key To Be Returned"],
      default: "Key Requested",
    },
    accessLog: {
      type: [String],
      default: [],
    },
    isReturned: {
      type: Boolean,
      default: false,
    },
    returnedAt: {
      type: Date,
      default: null,
    },
    rawData: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
  }
);

export default keySchema;
