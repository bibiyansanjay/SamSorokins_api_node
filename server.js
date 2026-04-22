import "./db/config.js";
import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import fs from "fs";
import path from "path";
import util from "util";

// =========================
// ✅ Global Console Override
// =========================
const logsDir = path.join(process.cwd(), "logs");
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir);
}
const appLogPath = path.join(logsDir, "app.log");
const errLogPath = path.join(logsDir, "error.log");

const originalLog = console.log;
const originalError = console.error;
const originalWarn = console.warn;

console.log = function (...args) {
  const msg = util.format(...args);
  fs.appendFile(appLogPath, `${new Date().toISOString()} [INFO]  - ${msg}\n`, () => {});
  originalLog.apply(console, args);
};

console.error = function (...args) {
  const msg = util.format(...args);
  fs.appendFile(errLogPath, `${new Date().toISOString()} [ERROR] - ${msg}\n`, () => {});
  originalError.apply(console, args);
};

console.warn = function (...args) {
  const msg = util.format(...args);
  fs.appendFile(appLogPath, `${new Date().toISOString()} [WARN]  - ${msg}\n`, () => {});
  originalWarn.apply(console, args);
};

import routes from "./routes";
import db from "./db";
import isTokenExpired from "./middlewares/isTokenExpired.js";
import createDefaultUser from "./db/DefaultUser.js";

// Initialize app
const app = express();

// Load env variables
dotenv.config();

// Connect to database
db();

// Initialize Cron Jobs
import "./cron/uploadReminder.js";
import "./cron/uploadCleanup.js";

// Create default user
createDefaultUser();

// =========================
// ✅ Error Logging Function
// =========================
const logErrorToFile = (errorDetails) => {
  const logsDir = path.join(process.cwd(), "logs");
  const logFilePath = path.join(logsDir, "error.log");

  // Create logs directory if not exists
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir);
  }

  const logEntry = `${new Date().toISOString()} - ERROR: ${
    errorDetails.message
  }, CODE: ${errorDetails.code || "N/A"}, STATUS: ${
    errorDetails.status || 500
  }, URL: ${errorDetails.url}, METHOD: ${errorDetails.method}, USER: ${
    errorDetails.user || "Guest"
  }, ROLE: ${errorDetails.role || "N/A"}, IP: ${
    errorDetails.ip || "Unknown"
  }\n`;

  fs.appendFile(logFilePath, logEntry, (err) => {
    if (err) {
      console.error("Failed to write error to log file:", err);
    }
  });
};

// =========================
// ✅ Middleware
// =========================
app.use(express.json());

app.use(
  cors({
    origin: "*",
    methods: ["POST", "PUT", "PATCH", "HEAD", "OPTIONS", "DELETE"],
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

// =========================
// ✅ Routes
// =========================
app.use("/", isTokenExpired, routes);

// =========================
// ✅ Global Error Handler (MUST BE LAST)
// =========================
app.use((err, req, res, next) => {
  const clientIP =
    req.headers["x-forwarded-for"]?.split(",")[0] || req.ip || "Unknown IP";

  logErrorToFile({
    message: err?.message || "Unknown error",
    code: err?.code || "N/A",
    status: err?.status || 500,
    url: req?.originalUrl,
    method: req?.method,

    ip: clientIP,
  });

  res.status(err?.status || 500).json({
    success: false,
    message: err?.message || "Internal Server Error",
  });
});

// =========================
// ✅ Start Server
// =========================
const PORT = process.env.PORT || 8000;

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
