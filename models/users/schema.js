import mongoose from "mongoose";

/**
 * @method userSchema
 * @memberof module:Schema
 * @description User schema is define here
 */

const userSchema = new mongoose.Schema(
  {
    firstName: {
      type: String,
      trim: true,
      required: true,
    },
    lastName: {
      type: String,
      trim: true,
      required: false,
    },
    email: {
      type: String,
      required: true,
      // unique: true,
    },
    role: {
      type: String,
      default: "CLIENT",
      enum: ["ADMIN", "CLIENT", "SUB-ADMIN"],
    },
    password: {
      type: String,
      required: false,
    },
    phoneNumber: {
      type: String,
      required: false,
    },
    profileImg: {
      type: String,
      default: "",
    },
    signature: {
      type: String,
      default: "",
    },
    isDeleted: {
      type: Boolean,
      default: false,
    },
    isLoggedIn: {
      type: Boolean,
      default: false,
    },
    otp: {
      type: String,
      required: false,
    },
    country: {
      type: String,
      required: false,
    },
    address: {
      type: String,
      required: false,
    },
    state: {
      type: String,
      required: false,
    },
    city: {
      type: String,
      required: false,
    },
    pinCode: {
      type: String,
      required: false,
    },
    gender: {
      type: String,
      required: false,
      enum: ["male", "female", "other"],
    },
    lastActive: {
      type: Date,
      default: Date.now,
    },
    resetPasswordToken: {
      type: String,
      required: false,
      default: "",
    },
    resetPasswordExpires: {
      type: String,
      required: false,
      default: "",
    },
    walletPrice: {
      type: Number,
      default: 0,
    },
    createdBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User", // Reference to the User model
    },
  },
  { timestamps: true }
);

export default userSchema;
