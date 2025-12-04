import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import routes from "./routes";
import db from "./db";
import isTokenExpired from "./middlewares/isTokenExpired";
// require("./cron/scheduledTasks");

const app = express();

// Configuring environment variables from .env file
dotenv.config();

//Connect to database
//db();

// Middleware
app.use(express.json());

// CORS middleware setup for Express
app.use(cors());

// Routes
app.use("/", isTokenExpired, routes);

// Port setup
const PORT = process.env.PORT || 5000;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
