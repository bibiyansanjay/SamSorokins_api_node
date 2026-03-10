import jwt from "jsonwebtoken";

export default function () {
  const data = { id: this.id };

  // Set the expiration time for the token to 1 week
  const expiresIn = "24h";

  return jwt.sign(data, process.env.JWT_SECRET, { expiresIn });
}
