// import { connectDB } from "@/lib/mongodb";
import { User } from "../models";
// import User from "../models/users";

/**
 * Create default admin user (idempotent)
 */
const createDefaultUser = async () => {
  // await connectDB();

  const { DEFAULT_USER_EMAIL, DEFAULT_USER_PASSWORD, TWILIO_TO_PHONE } =
    process.env;

  if (!DEFAULT_USER_EMAIL || !DEFAULT_USER_PASSWORD) {
    throw new Error("Default admin credentials are missing in env");
  }

  try {
    const existingAdmin = await User.findOne({
      email: DEFAULT_USER_EMAIL,
    });

    if (existingAdmin) {
      console.log("✅ Default admin already exists");
      return;
    }

    const admin = new User({
      firstName: "Admin",
      lastName: "Support",
      role: "Admin",
      email: DEFAULT_USER_EMAIL,
      password: DEFAULT_USER_PASSWORD, // 🔐 WILL BE HASHED
      isSuperAdmin: true,
      isActive: true,
      visible: true,
      // canChangeEmail: true,
      // canChangeMobileNumber: true,
      // canChangePhoneNumber: true,
      // canChangePassword: true,
      // mobileNumber: TWILIO_TO_PHONE,
      recordId: "U00001",
      isFirstLogin: false,
    });

    await admin.save(); // ✅ pre("save") fires here

    console.log("✅ Default admin created:", admin.email);
  } catch (error) {
    console.error("❌ Error creating default admin:", error);
  }
};

export default createDefaultUser;
