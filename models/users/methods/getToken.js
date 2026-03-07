import jwt from "jsonwebtoken";

export default function () {
  const data = { id: this.id };

  return jwt.sign(data, process.env.JWT_SECRET);
}
