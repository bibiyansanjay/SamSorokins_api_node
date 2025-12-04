import jwt from "jsonwebtoken";

export default function (userID) {
  const data = { id: userID };
  console.log("Data for JWT:", data);
  return jwt.sign(data, process.env.JWT_SECRET);
}
