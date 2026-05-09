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

// Log rotation helpers
const getYearMonth = (date) =>
  `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
const getLogPaths = () => {
  const ym = getYearMonth(new Date());
  return {
    appLogPath: path.join(logsDir, `app-${ym}.log`),
    errLogPath: path.join(logsDir, `error-${ym}.log`),
  };
};

const cleanupOldLogs = () => {
  try {
    const files = fs.readdirSync(logsDir);
    const now = new Date();
    const currentYM = getYearMonth(now);

    // Calculate previous month
    const prevDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const prevYM = getYearMonth(prevDate);

    const validMatches = [
      `app-${currentYM}.log`,
      `error-${currentYM}.log`,
      `app-${prevYM}.log`,
      `error-${prevYM}.log`,
    ];

    for (const file of files) {
      if (
        (file.startsWith("app-") || file.startsWith("error-")) &&
        file.endsWith(".log") &&
        !validMatches.includes(file)
      ) {
        try {
          fs.unlinkSync(path.join(logsDir, file));
        } catch (e) {
          console.error(`Failed to delete old log file ${file}:`, e);
        }
      }
    }
  } catch (err) {
    console.error("Failed to cleanup old logs:", err);
  }
};

// Cleanup on startup and schedule every 24 hours
cleanupOldLogs();
setInterval(cleanupOldLogs, 24 * 60 * 60 * 1000);

const originalLog = console.log;
const originalError = console.error;
const originalWarn = console.warn;

console.log = function (...args) {
  const msg = util.format(...args);
  fs.appendFile(
    getLogPaths().appLogPath,
    `${new Date().toISOString()} [INFO]  - ${msg}\n`,
    () => {}
  );
  originalLog.apply(console, args);
};

console.error = function (...args) {
  const msg = util.format(...args);
  fs.appendFile(
    getLogPaths().errLogPath,
    `${new Date().toISOString()} [ERROR] - ${msg}\n`,
    () => {}
  );
  originalError.apply(console, args);
};

console.warn = function (...args) {
  const msg = util.format(...args);
  fs.appendFile(
    getLogPaths().appLogPath,
    `${new Date().toISOString()} [WARN]  - ${msg}\n`,
    () => {}
  );
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
import "./cron/dailyUploadReport.js";

// Create default user
createDefaultUser();

// =========================
// ✅ Error Logging Function
// =========================
const logErrorToFile = (errorDetails) => {
  const logsDir = path.join(process.cwd(), "logs");
  const d = new Date();
  const ym = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  const logFilePath = path.join(logsDir, `error-${ym}.log`);

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

app.use(express.urlencoded({ extended: true }));

app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "PUT", "PATCH", "HEAD", "OPTIONS", "DELETE"],
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
