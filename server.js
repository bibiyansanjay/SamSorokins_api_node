import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import routes from "./routes";
import db from "./db";
import isTokenExpired from "./middlewares/isTokenExpired";
import createDefaultUser from "./db/DefaultUser.js";

const app = express();

// Configuring environment variables from .env file
dotenv.config();

// Connect to database
db();

// Initialize Cron Jobs
import "./cron/uploadReminder.js";

// Create default user
createDefaultUser();

// Middleware
app.use(express.json());

// CORS middleware setup for Express
// app.use(cors());
// CORS middleware setup for Express
app.use(
  cors({
    origin: "*",
    methods: ["POST", "PATCH", "HEAD", "OPTIONS"],
    allowedHeaders: [
      "Tus-Resumable",
      "Upload-Length",
      "Upload-Metadata",
      "Upload-Offset",
      "Content-Type",
      "Accept",
      "Origin",
      "Authorization",
      "X-Requested-With",
    ],
    exposedHeaders: ["Location", "Upload-Offset", "Upload-Length"],
  })
);

// Routes
app.use("/", isTokenExpired, routes);

// Port setup
const PORT = process.env.PORT || 8000;

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
