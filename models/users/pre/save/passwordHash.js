import bcrypt from "bcrypt";

const saltRounds = 10;

export default async function (password, next) {
  try {
    const salt = await bcrypt.genSalt(saltRounds);
    const hash = await bcrypt.hash(password, salt);
    // next();
    return hash;
  } catch (error) {
    next(error);
  }
}
