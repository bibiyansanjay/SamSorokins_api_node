import mongoose from "mongoose";
import keySchema from "./schema";

const Key = mongoose.model("Key", keySchema);

export default Key;
