import mongoose from "mongoose";
import userSchema from "./schema";
import methods from "./methods";
import pre from "./pre";
import virtuals from "./virtuals";

// Pre save
userSchema.pre("save", pre.save.passwordHash);

//virtuals
userSchema.virtual("fullName").get(virtuals.fullName);

userSchema.set("toJSON", { virtuals: true });
userSchema.set("toObject", { virtuals: true });

// Methods
Object.assign(userSchema.methods, methods);

//User model created
const User = mongoose.model("Users", userSchema);

export default User;
