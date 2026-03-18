import jwt from "jsonwebtoken";

export default function () {
  const data = { id: this.id };

  // Set the expiration time for the token to 1 week
  // const expiresIn = "24h";
  // "7d"     // 7 days ✅ (recommended)
  // "168h"   // 168 hours (7×24)
  // "10080m" // minutes
  // "604800s" // seconds
  const expiresIn = "7d";

  return jwt.sign(data, process.env.JWT_SECRET, { expiresIn });
}
