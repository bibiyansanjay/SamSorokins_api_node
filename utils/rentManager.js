import axios from "axios";
import { google } from "googleapis";
import fs from "fs";
import path from "path";
import util from "util";
import { WebhookErrorLog } from "../models/index.js";

let rmTokenCache = { token: null, expiresAt: 0 };

/**
 * Authenticates with the Rent Manager API and returns a valid token.
 * Caches the token in memory and reuses it until it expires.
 */
export async function getRMToken() {
  const now = Date.now();

  // Use cached token if still valid
  if (rmTokenCache.token && now < rmTokenCache.expiresAt) {
    console.log("[RM Auth] Using cached token.");
    return rmTokenCache.token;
  }

  const maxRetries = 3;
  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(
        `[RM Auth] Fetching new token (Attempt ${attempt}/${maxRetries})...`
      );
      const response = await axios.post(
        `${process.env.RM_BASE_URL}/Authentication/AuthorizeUser`,
        {
          Username: process.env.RM_USERNAME,
          Password: process.env.RM_PASSWORD,
          LocationId: parseInt(process.env.RM_LOCATION_ID, 10),
        },
        {
          headers: { "Content-Type": "application/json" },
          timeout: 10000, // 10 second timeout
        }
      );

      const token = response.data;
      if (!token) throw new Error("Rent Manager returned an empty token.");

      // Cache token for 14 minutes
      rmTokenCache = {
        token,
        expiresAt: now + 14 * 60 * 1000,
      };

      console.log("[RM Auth] New token acquired and cached.");
      return token;
    } catch (error) {
      lastError = error;
      console.warn(`[RM Auth] Attempt ${attempt} failed: ${error.message}`);

      if (attempt < maxRetries) {
        const delay = attempt * 1000; // 1s, 2s delay
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  throw new Error(
    `Failed to acquire Rent Manager token after ${maxRetries} attempts. Last error: ${lastError.message}`
  );
}

/**
 * Returns axios headers pre-loaded with the RM auth token.
 */
export async function getRMHeaders() {
  const token = await getRMToken();
  return {
    "Content-Type": "application/json",
    "X-RM12Api-ApiToken": token,
  };
}

/**
 * Reads Google Sheets and returns all config rows matching the given formId.
 */
export async function getMatchingRows(targetFormId) {
  const normalizedTargetId = String(targetFormId || "").trim();
  console.log(`[Sheets] Looking up config for formId: "${normalizedTargetId}"`);

  const sheetsAuth = new google.auth.GoogleAuth({
    keyFile: "./rent-manager-dont-delete-6d156143cf65.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  const sheets = google.sheets({ version: "v4", auth: sheetsAuth });

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.SPREADSHEET_ID,
      range: "Sheet1!A:K",
    });

    const rows = response.data.values;
    if (!rows || rows.length === 0) {
      console.log("[Sheets] No rows found in spreadsheet.");
      return [];
    }

    return rows
      .slice(1) // Skip header row
      .filter((row) => {
        const rowStatus = String(row[0] || "")
          .toLowerCase()
          .trim();
        const rowFormId = String(row[1] || "").trim();
        return rowFormId === normalizedTargetId && rowStatus === "final";
      })
      .map((row) => ({
        status: row[0],
        formId: row[1],
        itemType: row[3], // get the item type (e.g. "PDF", "Text", "Number")
        item: row[4], // The value to use
        extraInfo: row[5], // The value to use
        field: row[7], // The UDF name
        action: (row[9] || "").toLowerCase().trim(), // replace | prepend | empty
      }));
  } catch (error) {
    console.error(`[Sheets] Error fetching rows: ${error.message}`);
    throw error;
  }
}

/**
 * Fetches a Jotform submission by ID and returns the full content object.
 */
export async function getJotformSubmission(submissionId) {
  console.log(`[Jotform] Fetching submission ID: ${submissionId}`);
  const response = await axios.get(
    `${process.env.JOTFORM_BASE_URL}/API/submission/${submissionId}?apiKey=${process.env.JOTFORM_API_KEY}`
  );

  if (response.data.responseCode !== 200) {
    throw new Error(`Jotform API error: ${response.data.message}`);
  }

  return response.data.content;
}

/**
 * Resolves a Jotform answer value from the answers dictionary.
 */
export function resolveJotformAnswer(answers, item) {
  if (!item || !answers) return "";

  const subFieldMatch = item.match(/^q?(\d+)_(.+)$/i);
  if (subFieldMatch) {
    const qNum = subFieldMatch[1];
    const subKey = subFieldMatch[2];
    const answer = answers[qNum]?.answer;
    if (answer && typeof answer === "object") {
      return String(answer[subKey] || "");
    }
  }

  const qNum = item.replace(/^q/i, "");
  const answer = answers[qNum]?.answer;

  if (!answer) return "";
  if (typeof answer === "string") return answer.trim();
  if (typeof answer === "object") {
    return Object.values(answer)
      .filter((v) => v && String(v).trim())
      .join(" ")
      .trim();
  }
  return String(answer);
}

/**
 * Logs Rent Manager actions to a monthly log file.
 */
export function logRMAction(data) {
  const logsDir = path.join(process.cwd(), "logs");
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir);
  }

  const d = new Date();
  const ym = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  const logFilePath = path.join(logsDir, `rentmanager-${ym}.log`);

  const timestamp = d.toISOString();
  const entry = {
    timestamp,
    ...data,
  };

  const logLine = `${timestamp} - ${JSON.stringify(entry)}\n`;

  fs.appendFile(logFilePath, logLine, (err) => {
    if (err) console.error("[Logger] Failed to write RM log:", err);
  });

  // If this is an error/failure, save it to the DB as well
  if (
    data.error ||
    data.type?.includes("FAILURE") ||
    data.type?.includes("NOT_FOUND") ||
    data.status === "error"
  ) {
    WebhookErrorLog.create({
      email: data?.email || "",
      formId: data?.formId || "",
      submissionID: data?.submissionID || "",
      type: data?.type || "UNKNOWN_ERROR",
      reason: data?.error || data?.reason || "No explicit reason provided",
      details: data,
    }).catch((dbErr) => {
      console.error("[Logger] Failed to save error log to DB:", dbErr);
    });
  }
}
