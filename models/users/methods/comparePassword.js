import bcrypt from "bcrypt";

export default function (userPassword) {
  return bcrypt.compare(userPassword, this.password);
}
