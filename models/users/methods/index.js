import sendOTP from "../../../routes/auth/sendOTP";
import comparePassword from "./comparePassword";
import getToken from "./getToken";
import getUserByEmail from "./getUserByEmail";

export default {
  getToken,
  comparePassword,
  sendOTP,
  getUserByEmail
};
