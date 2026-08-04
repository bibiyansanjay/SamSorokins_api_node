import mongoose from "mongoose";

const keySchema = new mongoose.Schema(
  {
    vendor: {
      type: String,
      required: true,
      trim: true,
      default: "",
    },
    phoneNumber: {
      type: String,
      trim: true,
      default: "",
    },
    email: {
      type: String,
      trim: true,
      default: "",
    },
    repairsEmail: {
      type: String,
      trim: true,
      default: "repairs@premiumpd.com",
    },
    property: {
      type: String,
      required: true,
      trim: true,
      default: "",
    },
    unit: {
      type: String,
      trim: true,
      default: "",
    },
    serviceIssue: {
      type: String,
      trim: true,
      default: "",
    },
    fullAddress: {
      type: String,
      trim: true,
      default: "",
    },
    pickUpDateTime: {
      type: String,
      trim: true,
      default: "",
    },
    byWhen: {
      type: String,
      trim: true,
      default: "",
    },
    keysNeeded: {
      type: String,
      trim: true,
      default: "",
    },
    // requestType: {
    //   type: String,
    //   enum: ["Vendor", "Non-Vendor"],
    //   default: "Vendor",
    // },
    userType: {
      type: String,
      enum: ["Vendor", "Non-Vendor"],
      default: "Vendor",
    },
    purpose: {
      type: String,
      trim: true,
      default: "",
    },
    purposeDescription: {
      type: String,
      trim: true,
      default: "",
    },
    areKeysForYou: {
      type: String,
      trim: true,
      default: "",
    },
    whoWillPickUp: {
      type: String,
      trim: true,
      default: "",
    },
    pickerPhoneNumber: {
      type: String,
      trim: true,
      default: "",
    },
    pickerEmail: {
      type: String,
      trim: true,
      default: "",
    },
    willBeReturned: {
      type: String,
      trim: true,
      default: "",
    },
    whyNotReturned: {
      type: String,
      trim: true,
      default: "",
    },
    status: {
      type: String,
      enum: [
        "Checked Out",
        "Checked In",
        // "Outstanding",
        "Checked Out Permanently",
        "Lost",
        "Requested",
        "To Be Returned",
      ],
      default: "Requested",
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
    isDeleted: {
      type: Boolean,
      default: false,
    },
    deletedAt: {
      type: Date,
      default: null,
    },
    rawData: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    source: {
      //to track where it was created, from system or webhook
      type: String,
      enum: ["System", "Webhook"],
      default: "System",
    },
    rfId: {
      type: String,
      trim: true,
      default: "",
    },
    isAccountingEmailSent: {
      type: Boolean,
      default: false,
    },
    accountingEmailSentAt: {
      type: Date,
      default: null,
    },
    weeklyReminderSentAt: {
      type: Date,
      default: null,
    },
  },
  {
    timestamps: true,
    strict: false,
  }
);

export default keySchema;
