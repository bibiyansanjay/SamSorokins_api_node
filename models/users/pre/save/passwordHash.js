import bcrypt from "bcrypt";

const saltRounds = 10;

/**
 * @module Methods
 * @description All methods define here.
 */
export default async function (next) {
  const user = this;

  if (!user.isModified("password")) return next();

  try {
    const salt = await bcrypt.genSalt(saltRounds);

    const hash = await bcrypt.hash(user.password, salt);

    user.password = hash;
    next();
  } catch (error) {
    next(error);
  }
}
