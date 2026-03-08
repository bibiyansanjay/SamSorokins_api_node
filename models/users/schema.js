/**
 * @Function Mongoose Schema of Users
 * @Description Mongoose Schema of Users
 */

import { date } from "joi";
import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    recordId: {
      type: String,
      required: true,
    },
    suffix: {
      type: String,
      default: "",
      required: false,
    },
    firstName: {
      type: String,
      trim: true,
      required: true,
    },
    middleName: {
      type: String,
      trim: true,
    },
    lastName: {
      type: String,
      trim: true,
      required: true,
    },
    dob: {
      type: Date,
      default: null,
    },
    fullNameAtBirth: {
      type: String,
      default: "",
    },
    gender: {
      type: String,
      trim: true,
      // required: true,
    },
    phoneNumber: {
      type: String,
      default: "",
    },

    email: {
      type: String,
      required: true,
    },
    password: {
      type: String,
      required: true,
    },
    role: {
      type: String,
      enum: ["Admin", "Case_Manager", "Client"],
      required: true,
    },

    profileImg: {
      type: String,
      required: false,
      default: "",
    },
    signature: {
      type: String,
      default: "",
      required: false,
    },
    isActive: {
      type: Boolean,
      required: true,
    },

    isDeleted: {
      type: Boolean,
      default: false,
      // required: true,
    },

    isLoggedIn: {
      type: Boolean,
      // required: true,
      default: false,
    },
    isFirstLogin: {
      type: Boolean,
      default: true,
    },
    otp: {
      type: String,
      default: "",
      required: false,
    },
    otpExpiresAt: {
      type: Date,
      default: null,
    },
    resetPasswordToken: {
      type: String,
      default: "",
      required: false,
    },
    resetPasswordExpires: {
      type: String,
      default: "",
      required: false,
    },

    resendOtpCount: {
      type: Number,
      default: 0,
    },
    isPasswordChanged: {
      type: Boolean,
      default: false,
    },
    isPasswordChangedByAdmin: {
      type: Boolean,
      default: false,
    },
    isSuperAdmin: {
      type: Boolean,
      default: false,
    },

    lastActive: {
      type: Date,
    },

    forgotPasswordToken: {
      type: String,
      required: false,
      default: "",
    },

    address: {
      type: String,
      // required: true,
      default: "",
    },
    city: {
      type: String,
      // required: true,
      default: "",
    },
    country: {
      type: String,
      // required: true,
      default: "",
    },
    state: {
      type: String,
      // required: true,
      default: "",
    },
    pinCode: {
      type: String,
      default: "",
    },
  },
  {
    timestamps: true,
  }
);

export default userSchema;
