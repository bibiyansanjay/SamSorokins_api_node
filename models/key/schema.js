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
    serviceStatus: {
      type: String,
      trim: true,
      default: "",
    },
    status: {
      type: String,
      enum: [
        "Checked Out",
        "Checked In",
        "Outstanding",
        "Checked Out Permanently",
        "Lost",
        "Key Requested",
        "Key Checked Out",
        "Key To Be Returned",
      ],
      default: "Checked Out",
    },
    lostReason: {
      type: String,
      trim: true,
      default: "",
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
    source: {
      //to track the key where it create, from system or webhook
      type: String,
      enum: ["System", "Webhook"],
      default: "System",
    },
  },
  {
    timestamps: true,
  }
);

export default keySchema;
