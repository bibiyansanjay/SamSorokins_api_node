import crypto from "crypto";

const generateOtp = () => {
  return new Promise((resolve, reject) => {
    try {
      const otp = crypto.randomInt(1000, 10000).toString();
      resolve(otp);
    } catch (err) {
      reject(err);
    }
  });
};

export default generateOtp;
